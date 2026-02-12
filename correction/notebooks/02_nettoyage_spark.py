#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
ECF - Titre Professionnel Data Engineer (RNCP35288)
Competences evaluees : C2.1, C2.2, C2.3, C2.4
==============================================================================

Script 02 : Pipeline de nettoyage des donnees de consommation energetique
             avec Apache Spark

Description :
    Ce script charge le fichier consommations_raw.csv (~7.7M lignes),
    applique un pipeline complet de nettoyage (formats de dates multiples,
    valeurs textuelles, separateurs decimaux, valeurs negatives, outliers,
    doublons), calcule des agregations (horaires, journalieres, mensuelles)
    et exporte les donnees nettoyees au format Parquet partitionne.

Entree  : ../data/consommations_raw.csv
Sortie  : ../output/consommations_clean/ (Parquet partitionne par annee_mois et type_energie)

Execution :
    cd notebooks
    python 02_nettoyage_spark.py
    # ou
    spark-submit 02_nettoyage_spark.py

Auteur  : Candidat Data Engineer - ECF RNCP35288
==============================================================================
"""

# =============================================================================
# Imports
# =============================================================================
import os
import sys
import time
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    udf, col, regexp_replace, lower, trim, when,
    year, month, dayofmonth, hour, date_format,
    avg, sum as spark_sum, count, countDistinct,
    concat_ws, lit, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.window import Window


# =============================================================================
# Configuration du logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NettoyageSpark")


# =============================================================================
# Chemins des fichiers (relatifs au repertoire notebooks/)
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "output")

CHEMIN_CONSOMMATIONS = os.path.join(DATA_DIR, "consommations_raw.csv")
CHEMIN_BATIMENTS = os.path.join(DATA_DIR, "batiments.csv")
CHEMIN_SORTIE_CLEAN = os.path.join(OUTPUT_DIR, "consommations_clean")
CHEMIN_SORTIE_AGREGEES = os.path.join(OUTPUT_DIR, "consommations_agregees.parquet")

# Seuil maximal de consommation (au-dela = outlier)
SEUIL_OUTLIER = 10000

# Valeurs textuelles a considerer comme nulles
VALEURS_TEXTUELLES = ["erreur", "n/a", "---", "null", "na", "none", ""]

# Formats de dates a tenter lors du parsing
FORMATS_DATES = [
    "%Y-%m-%d %H:%M:%S",       # ISO : 2023-12-21 13:00:00
    "%d/%m/%Y %H:%M",          # FR  : 27/03/2023 13:00
    "%m/%d/%Y %H:%M:%S",       # US  : 06/13/2024 11:00:00
    "%Y-%m-%dT%H:%M:%S",       # ISO-T : 2023-12-21T13:00:00
]


# =============================================================================
# Fonctions utilitaires
# =============================================================================

def creer_session_spark() -> SparkSession:
    """
    Cree et configure une SparkSession optimisee pour le traitement
    de gros volumes de donnees (~7.7M lignes).
    """
    logger.info("Creation de la SparkSession...")

    spark = (
        SparkSession.builder
        .appName("ECF_Energie_Nettoyage_Consommations")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )

    # Reduire la verbosité des logs Spark
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession creee avec succes (master=local[*], driver.memory=4g)")
    return spark


def charger_donnees_csv(spark: SparkSession, chemin: str, nom: str) -> DataFrame:
    """
    Charge un fichier CSV en DataFrame Spark.
    Toutes les colonnes sont chargees en tant que StringType pour
    permettre un nettoyage fin avant la conversion de types.
    """
    chemin_absolu = os.path.abspath(chemin)
    logger.info(f"Chargement du fichier : {chemin_absolu}")

    if not os.path.exists(chemin_absolu):
        logger.error(f"Fichier introuvable : {chemin_absolu}")
        sys.exit(1)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")   # On garde tout en string
        .option("encoding", "UTF-8")
        .csv(chemin_absolu)
    )

    nb_lignes = df.count()
    nb_colonnes = len(df.columns)
    logger.info(f"  -> {nom} : {nb_lignes:,} lignes, {nb_colonnes} colonnes")
    logger.info(f"  -> Colonnes : {df.columns}")

    return df


# =============================================================================
# UDF de parsing des dates multi-format
# =============================================================================

def parser_date_multi_format(valeur_date: str):
    """
    Tente de parser une chaine de date selon plusieurs formats connus.
    Retourne un objet datetime ou None si aucun format ne correspond.

    Formats supportes :
        - ISO   : %Y-%m-%d %H:%M:%S  (ex: 2023-12-21 13:00:00)
        - FR    : %d/%m/%Y %H:%M     (ex: 27/03/2023 13:00)
        - US    : %m/%d/%Y %H:%M:%S  (ex: 06/13/2024 11:00:00)
        - ISO-T : %Y-%m-%dT%H:%M:%S  (ex: 2023-12-21T13:00:00)
    """
    if valeur_date is None or str(valeur_date).strip() == "":
        return None

    valeur_date = str(valeur_date).strip()

    for fmt in FORMATS_DATES:
        try:
            return datetime.strptime(valeur_date, fmt)
        except (ValueError, TypeError):
            continue

    return None


# Enregistrement de la UDF pour Spark
udf_parser_date = udf(parser_date_multi_format, TimestampType())


def est_valeur_textuelle(valeur: str) -> bool:
    """
    Verifie si une valeur de consommation est en realite du texte
    invalide (erreur, N/A, ---, null, etc.).
    """
    if valeur is None:
        return True
    valeur_nettoyee = str(valeur).strip().lower()
    return valeur_nettoyee in VALEURS_TEXTUELLES


udf_est_texte = udf(est_valeur_textuelle, StringType())


# =============================================================================
# Fonctions du pipeline de nettoyage
# =============================================================================

def nettoyer_timestamps(df: DataFrame) -> tuple:
    """
    Etape 1 : Parsing des timestamps multi-format via UDF.
    Les lignes dont le timestamp ne peut pas etre parse sont supprimees.

    Retourne :
        - DataFrame avec colonne timestamp convertie
        - Nombre de lignes supprimees (dates invalides)
    """
    logger.info("  [1/5] Parsing des timestamps multi-format...")

    nb_avant = df.count()

    # Application de la UDF de parsing
    df = df.withColumn("timestamp_parsed", udf_parser_date(col("timestamp")))

    # Comptage des dates invalides (non parsees)
    nb_dates_invalides = df.filter(col("timestamp_parsed").isNull()).count()

    # Suppression des lignes avec dates invalides
    df = (
        df.filter(col("timestamp_parsed").isNotNull())
        .drop("timestamp")
        .withColumnRenamed("timestamp_parsed", "timestamp")
    )

    logger.info(f"    -> Dates invalides supprimees : {nb_dates_invalides:,}")
    return df, nb_dates_invalides


def nettoyer_consommation(df: DataFrame) -> tuple:
    """
    Etape 2 : Nettoyage de la colonne consommation.
        - Remplacement des virgules decimales par des points
        - Filtrage des valeurs textuelles (erreur, N/A, ---, null)
        - Conversion en type Double

    Retourne :
        - DataFrame avec consommation nettoyee
        - Nombre de lignes supprimees (valeurs textuelles / non numeriques)
    """
    logger.info("  [2/5] Nettoyage de la colonne consommation...")

    nb_avant = df.count()

    # Trim et mise en minuscule pour la detection de texte
    df = df.withColumn("consommation_clean", trim(col("consommation")))

    # Detection des valeurs textuelles connues
    condition_texte = lower(col("consommation_clean")).isin(VALEURS_TEXTUELLES)
    nb_texte = df.filter(condition_texte).count()

    # Suppression des valeurs textuelles
    df = df.filter(~condition_texte)

    # Remplacement des virgules decimales par des points
    df = df.withColumn(
        "consommation_clean",
        regexp_replace(col("consommation_clean"), ",", ".")
    )

    # Conversion en Double (les valeurs non convertibles deviennent null)
    df = df.withColumn(
        "consommation_double",
        col("consommation_clean").cast(DoubleType())
    )

    # Comptage des valeurs qui n'ont pas pu etre converties en nombre
    nb_non_numerique = df.filter(col("consommation_double").isNull()).count()

    # Suppression des valeurs non numeriques restantes
    df = (
        df.filter(col("consommation_double").isNotNull())
        .drop("consommation", "consommation_clean")
        .withColumnRenamed("consommation_double", "consommation")
    )

    nb_total_supprime = nb_texte + nb_non_numerique

    logger.info(f"    -> Valeurs textuelles supprimees : {nb_texte:,}")
    logger.info(f"    -> Valeurs non numeriques supprimees : {nb_non_numerique:,}")
    return df, nb_total_supprime


def filtrer_valeurs_negatives(df: DataFrame) -> tuple:
    """
    Etape 3 : Suppression des valeurs de consommation negatives.
    Une consommation energetique ne peut pas etre negative.

    Retourne :
        - DataFrame filtre
        - Nombre de lignes supprimees
    """
    logger.info("  [3/5] Filtrage des valeurs negatives...")

    nb_negatives = df.filter(col("consommation") < 0).count()
    df = df.filter(col("consommation") >= 0)

    logger.info(f"    -> Valeurs negatives supprimees : {nb_negatives:,}")
    return df, nb_negatives


def filtrer_outliers(df: DataFrame, seuil: float = SEUIL_OUTLIER) -> tuple:
    """
    Etape 4 : Suppression des outliers dont la consommation depasse le seuil.
    Le seuil par defaut est de 10 000 (kWh ou m3).

    Retourne :
        - DataFrame filtre
        - Nombre de lignes supprimees
    """
    logger.info(f"  [4/5] Filtrage des outliers (consommation > {seuil:,.0f})...")

    nb_outliers = df.filter(col("consommation") > seuil).count()
    df = df.filter(col("consommation") <= seuil)

    logger.info(f"    -> Outliers supprimes : {nb_outliers:,}")
    return df, nb_outliers


def supprimer_doublons(df: DataFrame) -> tuple:
    """
    Etape 5 : Deduplication sur la cle (batiment_id, timestamp, type_energie).
    En cas de doublons, on conserve la premiere occurrence.

    Retourne :
        - DataFrame deduplique
        - Nombre de lignes supprimees
    """
    logger.info("  [5/5] Deduplication sur (batiment_id, timestamp, type_energie)...")

    nb_avant = df.count()

    # Fenetre de partition pour la deduplication
    fenetre_dedup = Window.partitionBy(
        "batiment_id", "timestamp", "type_energie"
    ).orderBy("consommation")

    df = (
        df.withColumn("rang_doublon", row_number().over(fenetre_dedup))
        .filter(col("rang_doublon") == 1)
        .drop("rang_doublon")
    )

    nb_apres = df.count()
    nb_doublons = nb_avant - nb_apres

    logger.info(f"    -> Doublons supprimes : {nb_doublons:,}")
    return df, nb_doublons


# =============================================================================
# Fonctions d'enrichissement et d'agregation
# =============================================================================

def ajouter_colonnes_temporelles(df: DataFrame) -> DataFrame:
    """
    Ajoute des colonnes temporelles derivees du timestamp pour
    faciliter les agregations et le partitionnement Parquet.
    """
    logger.info("Ajout des colonnes temporelles (annee, mois, jour, heure, annee_mois)...")

    df = (
        df
        .withColumn("annee", year(col("timestamp")))
        .withColumn("mois", month(col("timestamp")))
        .withColumn("jour", dayofmonth(col("timestamp")))
        .withColumn("heure", hour(col("timestamp")))
        .withColumn("date", col("timestamp").cast("date"))
        .withColumn("annee_mois", date_format(col("timestamp"), "yyyy-MM"))
    )

    return df


def calculer_agregation_horaire(df: DataFrame) -> DataFrame:
    """
    Agregation horaire : consommation moyenne par batiment et par heure.
    Utile pour identifier les pics de consommation dans la journee.
    """
    logger.info("Calcul de l'agregation horaire (moyenne par batiment et heure)...")

    df_horaire = (
        df.groupBy("batiment_id", "date", "heure", "type_energie")
        .agg(
            avg("consommation").alias("consommation_moyenne"),
            count("*").alias("nb_mesures")
        )
        .orderBy("batiment_id", "date", "heure")
    )

    logger.info(f"  -> Agregation horaire : {df_horaire.count():,} lignes")
    return df_horaire


def calculer_agregation_journaliere(df: DataFrame) -> DataFrame:
    """
    Agregation journaliere : consommation totale par batiment,
    par jour et par type d'energie.
    """
    logger.info("Calcul de l'agregation journaliere (somme par batiment, jour, type_energie)...")

    df_journalier = (
        df.groupBy("batiment_id", "date", "type_energie")
        .agg(
            spark_sum("consommation").alias("consommation_totale"),
            avg("consommation").alias("consommation_moyenne"),
            count("*").alias("nb_mesures")
        )
        .orderBy("batiment_id", "date", "type_energie")
    )

    logger.info(f"  -> Agregation journaliere : {df_journalier.count():,} lignes")
    return df_journalier


def calculer_agregation_mensuelle_commune(
    df: DataFrame, df_batiments: DataFrame
) -> DataFrame:
    """
    Agregation mensuelle par commune : necessite une jointure avec
    le referentiel batiments pour obtenir la commune.
    """
    logger.info("Calcul de l'agregation mensuelle par commune (jointure avec batiments)...")

    # Jointure avec le referentiel batiments pour obtenir la commune
    df_avec_commune = df.join(
        df_batiments.select("batiment_id", "commune"),
        on="batiment_id",
        how="left"
    )

    df_mensuel = (
        df_avec_commune.groupBy("commune", "annee_mois", "type_energie")
        .agg(
            spark_sum("consommation").alias("consommation_totale"),
            avg("consommation").alias("consommation_moyenne"),
            countDistinct("batiment_id").alias("nb_batiments"),
            count("*").alias("nb_mesures")
        )
        .orderBy("commune", "annee_mois", "type_energie")
    )

    logger.info(f"  -> Agregation mensuelle par commune : {df_mensuel.count():,} lignes")
    return df_mensuel


# =============================================================================
# Fonctions de sauvegarde
# =============================================================================

def sauvegarder_parquet_partitionne(
    df: DataFrame, chemin_sortie: str, colonnes_partition: list
) -> None:
    """
    Sauvegarde un DataFrame au format Parquet avec partitionnement.
    Le mode 'overwrite' ecrase les donnees existantes.
    """
    chemin_absolu = os.path.abspath(chemin_sortie)
    logger.info(f"Sauvegarde Parquet partitionne -> {chemin_absolu}")
    logger.info(f"  -> Partitions : {colonnes_partition}")

    (
        df.write
        .mode("overwrite")
        .partitionBy(*colonnes_partition)
        .parquet(chemin_absolu)
    )

    logger.info("  -> Sauvegarde Parquet terminee avec succes.")


def sauvegarder_agregations(
    df_horaire: DataFrame,
    df_journalier: DataFrame,
    df_mensuel: DataFrame,
    chemin_sortie: str
) -> None:
    """
    Sauvegarde les DataFrames d'agregation au format Parquet.
    Chaque agregation est stockee dans un sous-repertoire distinct.
    """
    chemin_absolu = os.path.abspath(chemin_sortie)

    # Agregation horaire
    chemin_horaire = os.path.join(chemin_absolu, "agregation_horaire")
    logger.info(f"Sauvegarde agregation horaire -> {chemin_horaire}")
    df_horaire.write.mode("overwrite").parquet(chemin_horaire)

    # Agregation journaliere
    chemin_journalier = os.path.join(chemin_absolu, "agregation_journaliere")
    logger.info(f"Sauvegarde agregation journaliere -> {chemin_journalier}")
    df_journalier.write.mode("overwrite").parquet(chemin_journalier)

    # Agregation mensuelle par commune
    chemin_mensuel = os.path.join(chemin_absolu, "agregation_mensuelle_commune")
    logger.info(f"Sauvegarde agregation mensuelle par commune -> {chemin_mensuel}")
    df_mensuel.write.mode("overwrite").parquet(chemin_mensuel)

    logger.info("Toutes les agregations ont ete sauvegardees avec succes.")


# =============================================================================
# Fonction principale d'affichage du rapport
# =============================================================================

def afficher_rapport(
    nb_lignes_entree: int,
    nb_dates_invalides: int,
    nb_texte_non_numerique: int,
    nb_negatives: int,
    nb_outliers: int,
    nb_doublons: int,
    nb_lignes_sortie: int,
    duree_secondes: float
) -> None:
    """
    Affiche un rapport de synthese du pipeline de nettoyage
    avec le detail des lignes supprimees par type de defaut.
    """
    nb_total_supprime = (
        nb_dates_invalides + nb_texte_non_numerique +
        nb_negatives + nb_outliers + nb_doublons
    )
    taux_retention = (nb_lignes_sortie / nb_lignes_entree * 100) if nb_lignes_entree > 0 else 0

    minutes = int(duree_secondes // 60)
    secondes = duree_secondes % 60

    rapport = f"""
{'=' * 70}
  RAPPORT DE NETTOYAGE - PIPELINE SPARK
  ECF Energie Batiments - Data Engineer RNCP35288
{'=' * 70}

  Fichier source       : consommations_raw.csv
  Lignes en entree     : {nb_lignes_entree:>12,}

  --- Detail des suppressions ---
  Dates invalides      : {nb_dates_invalides:>12,}
  Texte / non numerique: {nb_texte_non_numerique:>12,}
  Valeurs negatives    : {nb_negatives:>12,}
  Outliers (>{SEUIL_OUTLIER:,.0f})    : {nb_outliers:>12,}
  Doublons             : {nb_doublons:>12,}
                          {'─' * 12}
  Total supprime       : {nb_total_supprime:>12,}

  Lignes en sortie     : {nb_lignes_sortie:>12,}
  Taux de retention    : {taux_retention:>11.2f}%

  Format de sortie     : Parquet (partitionne par annee_mois, type_energie)
  Compression          : Snappy

  Duree du traitement  : {minutes}min {secondes:.1f}s
{'=' * 70}
"""
    # Affichage via print pour visibilite dans la console
    print(rapport)
    logger.info("Rapport de nettoyage affiche avec succes.")


# =============================================================================
# Pipeline principal
# =============================================================================

def executer_pipeline() -> None:
    """
    Fonction principale qui orchestre l'ensemble du pipeline de nettoyage :
        1. Creation de la session Spark
        2. Chargement des donnees brutes
        3. Application du pipeline de nettoyage (5 etapes)
        4. Enrichissement temporel
        5. Calcul des agregations
        6. Sauvegarde en Parquet partitionne
        7. Affichage du rapport de synthese
    """
    debut = time.time()

    logger.info("=" * 70)
    logger.info("DEMARRAGE DU PIPELINE DE NETTOYAGE SPARK")
    logger.info("ECF Energie Batiments - Data Engineer RNCP35288")
    logger.info("=" * 70)

    # -----------------------------------------------------------------
    # 1. Creation de la session Spark
    # -----------------------------------------------------------------
    spark = creer_session_spark()

    try:
        # -----------------------------------------------------------------
        # 2. Chargement des donnees brutes
        # -----------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("PHASE 1 : CHARGEMENT DES DONNEES")
        logger.info("-" * 50)

        df_raw = charger_donnees_csv(spark, CHEMIN_CONSOMMATIONS, "consommations_raw")
        df_batiments = charger_donnees_csv(spark, CHEMIN_BATIMENTS, "batiments")

        nb_lignes_entree = df_raw.count()

        # Apercu des donnees brutes
        logger.info("Apercu des donnees brutes (5 premieres lignes) :")
        df_raw.show(5, truncate=False)

        # -----------------------------------------------------------------
        # 3. Pipeline de nettoyage (5 etapes)
        # -----------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("PHASE 2 : PIPELINE DE NETTOYAGE (5 etapes)")
        logger.info("-" * 50)

        # Etape 1 : Parsing des timestamps
        df_clean, nb_dates_invalides = nettoyer_timestamps(df_raw)

        # Etape 2 : Nettoyage de la consommation
        df_clean, nb_texte_non_numerique = nettoyer_consommation(df_clean)

        # Etape 3 : Filtrage des valeurs negatives
        df_clean, nb_negatives = filtrer_valeurs_negatives(df_clean)

        # Etape 4 : Filtrage des outliers
        df_clean, nb_outliers = filtrer_outliers(df_clean)

        # Etape 5 : Deduplication
        df_clean, nb_doublons = supprimer_doublons(df_clean)

        nb_lignes_sortie = df_clean.count()

        # Apercu des donnees nettoyees
        logger.info("Apercu des donnees nettoyees (5 premieres lignes) :")
        df_clean.show(5, truncate=False)
        logger.info(f"Schema des donnees nettoyees :")
        df_clean.printSchema()

        # -----------------------------------------------------------------
        # 4. Enrichissement temporel
        # -----------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("PHASE 3 : ENRICHISSEMENT TEMPOREL")
        logger.info("-" * 50)

        df_clean = ajouter_colonnes_temporelles(df_clean)

        # -----------------------------------------------------------------
        # 5. Calcul des agregations
        # -----------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("PHASE 4 : CALCUL DES AGREGATIONS")
        logger.info("-" * 50)

        df_horaire = calculer_agregation_horaire(df_clean)
        df_journalier = calculer_agregation_journaliere(df_clean)
        df_mensuel = calculer_agregation_mensuelle_commune(df_clean, df_batiments)

        # -----------------------------------------------------------------
        # 6. Sauvegarde des resultats
        # -----------------------------------------------------------------
        logger.info("-" * 50)
        logger.info("PHASE 5 : SAUVEGARDE DES RESULTATS")
        logger.info("-" * 50)

        # Creation du repertoire de sortie si necessaire
        os.makedirs(os.path.abspath(OUTPUT_DIR), exist_ok=True)

        # Sauvegarde des donnees nettoyees (Parquet partitionne)
        sauvegarder_parquet_partitionne(
            df=df_clean,
            chemin_sortie=CHEMIN_SORTIE_CLEAN,
            colonnes_partition=["annee_mois", "type_energie"]
        )

        # Sauvegarde des agregations
        sauvegarder_agregations(
            df_horaire=df_horaire,
            df_journalier=df_journalier,
            df_mensuel=df_mensuel,
            chemin_sortie=CHEMIN_SORTIE_AGREGEES
        )

        # -----------------------------------------------------------------
        # 7. Rapport de synthese
        # -----------------------------------------------------------------
        duree = time.time() - debut

        afficher_rapport(
            nb_lignes_entree=nb_lignes_entree,
            nb_dates_invalides=nb_dates_invalides,
            nb_texte_non_numerique=nb_texte_non_numerique,
            nb_negatives=nb_negatives,
            nb_outliers=nb_outliers,
            nb_doublons=nb_doublons,
            nb_lignes_sortie=nb_lignes_sortie,
            duree_secondes=duree
        )

        logger.info("Pipeline de nettoyage termine avec succes.")

    except Exception as e:
        logger.error(f"Erreur lors de l'execution du pipeline : {e}")
        raise

    finally:
        # Fermeture propre de la session Spark
        logger.info("Fermeture de la SparkSession...")
        spark.stop()
        logger.info("SparkSession fermee. Fin du programme.")


# =============================================================================
# Point d'entree
# =============================================================================
if __name__ == "__main__":
    executer_pipeline()

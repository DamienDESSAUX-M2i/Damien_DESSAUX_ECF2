# ===
# Etape 1.2 : Pipeline de nettoyage Spark
# ===

# Competence evaluee : C2.2 - Traiter des donnees structurees avec un langage de programmation**

from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

PROJECT_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = PROJECT_DIR / "data_ecf"
OUTPUT_DIR = PROJECT_DIR / "output"
CONSOMMATIONS_PATH = DATA_DIR / "consommations_raw.csv"
BETIMENTS_PATH = DATA_DIR / "batiments.csv"


def create_spark_session() -> SparkSession:
    """Cree et configure la session Spark."""
    builder: SparkSession.Builder = SparkSession.builder
    spark = (
        builder.appName("TP Qualite Air - Nettoyage")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# Creer des UDF pour parser les timestamps multi-formats
def parse_multi_format_timestamp(timestamp_str: str) -> Optional[datetime]:
    """
    UDF pour parser les timestamps multi-formats.
    Formats supportes:
    - %Y-%m-%d %H:%M:%S (ISO)
    - %d/%m/%Y %H:%M (FR)
    - %m/%d/%Y %H:%M:%S (US)
    - %Y-%m-%dT%H:%M:%S (ISO avec T)
    """
    if timestamp_str is None:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    return None


# Convertir les valeurs avec virgule en float
def clean_value(value_str: str) -> Optional[float]:
    """
    UDF pour nettoyer les valeurs numeriques.
    - Remplace la virgule par un point
    - Retourne None pour les valeurs non numeriques
    """
    if value_str is None:
        return None

    try:
        clean_str = value_str.replace(",", ".")
        return float(clean_str)
    except (ValueError, AttributeError):
        return None


def main():
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")

    # Enregistrer les UDFs
    parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
    clean_value_udf = F.udf(clean_value, DoubleType())

    # Charger les donnees brutes
    print("\n[1/6] Chargement des donnees brutes...")
    df_raw = spark.read.option("header", "true").csv(CONSOMMATIONS_PATH.as_posix())

    initial_count = df_raw.count()
    print(f"  Lignes en entree: {initial_count:,}")

    df_batiments = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(BETIMENTS_PATH.as_posix())
    )

    # Parser les timestamps multi-formats
    print("\n[2/6] Parsing des timestamps multi-formats...")
    df_with_timestamp = df_raw.withColumn(
        "timestamp", parse_timestamp_udf(F.col("timestamp"))
    )

    # Filtrer les timestamps invalides
    invalid_timestamps = df_with_timestamp.filter(F.col("timestamp").isNull()).count()
    df_with_timestamp = df_with_timestamp.filter(F.col("timestamp").isNotNull())
    print(f"  Timestamps invalides supprimes: {invalid_timestamps:,}")

    # Convertir les valeurs avec virgule decimale en float
    print("\n[3/6] Conversion des valeurs numeriques...")
    df_with_consommations = df_with_timestamp.withColumn(
        "consommation", clean_value_udf(F.col("consommation"))
    )

    # Filtrer les valeurs non numeriques
    invalid_values = df_with_consommations.filter(
        F.col("consommation").isNull()
    ).count()
    df_with_consommations = df_with_consommations.filter(
        F.col("consommation").isNotNull()
    )
    print(f"  Valeurs non numeriques supprimees: {invalid_values:,}")

    # Filtrer les valeurs negatives et les outliers (>10000)
    print("\n[4/6] Suppression des valeurs aberrantes...")
    negative_count = df_with_consommations.filter(F.col("consommation") < 0).count()
    outlier_count = df_with_consommations.filter(F.col("consommation") > 10_000).count()

    df_clean = df_with_consommations.filter(
        (F.col("consommation") >= 0) & (F.col("consommation") <= 10_000)
    )
    print(f"  Valeurs negatives supprimees: {negative_count:,}")
    print(f"  Outliers (> 15 000) supprimes: {outlier_count:,}")

    # Dedupliquer sur (batiment_id, timestamp, type_energie)
    print("\n[5/6] Deduplication...")
    before_dedup = df_clean.count()
    df_dedup = df_clean.dropDuplicates(["batiment_id", "timestamp", "type_energie"])
    after_dedup = df_dedup.count()
    duplicates_removed = before_dedup - after_dedup
    print(f"  Doublons supprimes: {duplicates_removed:,}")

    # Calculer les moyennes horaires par bâtiment et type d'énergie
    print("\n[6/6] Agregation horaire et sauvegarde...")

    # Ajouter les colonnes de temps
    df_with_time = (
        df_dedup.withColumn("date", F.to_date(F.col("timestamp")))
        .withColumn("hour", F.hour(F.col("timestamp")))
        .withColumn("year", F.year(F.col("timestamp")))
        .withColumn("month", F.month(F.col("timestamp")))
    )

    # Calculer les agregations :
    #   Consommations horaires moyennes par batiment
    df_hourly = df_with_time.groupBy("batiment_id", "date", "hour").agg(
        F.round(F.mean("consommation"), 2).alias("consommation_mean_hour_bat"),
        F.round(F.min("consommation"), 2).alias("consommation_min_hour_bat"),
        F.round(F.max("consommation"), 2).alias("consommation_max_hour_bat"),
    )

    #   Consommations journalieres par batiment et type d'energie
    df_daily = df_with_time.groupBy("batiment_id", "type_energie", "date").agg(
        F.round(F.mean("consommation"), 2).alias("consommation_mean_day_bat_energy"),
        F.round(F.min("consommation"), 2).alias("consommation_min_day_bat_energy"),
        F.round(F.max("consommation"), 2).alias("consommation_max_day_bat_energy"),
    )

    # Joindre avec les informations des batiments
    df_join = df_with_time.join(df_batiments, on="batiment_id", how="left")

    #   Consommations mensuelles par commune
    df_monthly = df_join.groupBy("commune", "year", "month").agg(
        F.round(F.mean("consommation"), 2).alias("consommation_mean_month_com"),
        F.round(F.min("consommation"), 2).alias("consommation_min_month_com"),
        F.round(F.max("consommation"), 2).alias("consommation_max_month_com"),
    )

    df_final = (
        df_join.join(df_hourly, on=["batiment_id", "date", "hour"], how="left")
        .join(df_daily, on=["batiment_id", "type_energie", "date"], how="left")
        .join(df_monthly, on=["commune", "year", "month"], how="left")
    )

    # Sauvegarder en Parquet partitionne par date et type d'energie
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_final.write.mode("overwrite").partitionBy(["date", "type_energie"]).parquet(
        "/output/consommation_clean"
    )

    final_count = df_final.count()

    # Rapport final
    print("RAPPORT DE NETTOYAGE")
    print(f"Lignes en entree:              {initial_count:>12,}")
    print(f"Timestamps invalides:          {invalid_timestamps:>12,}")
    print(f"Valeurs non numeriques:        {invalid_values:>12,}")
    print(f"Valeurs negatives:             {negative_count:>12,}")
    print(f"Outliers (> 10 000):           {outlier_count:>12,}")
    print(f"Doublons:                      {duplicates_removed:>12,}")
    total_removed = (
        invalid_timestamps
        + invalid_values
        + negative_count
        + outlier_count
        + duplicates_removed
    )
    print(f"Total lignes supprimees:       {total_removed:>12,}")
    print(f"Lignes apres agregation:       {final_count:>12,}")
    print(f"\nFichiers Parquet sauvegardes dans: {OUTPUT_DIR}")

    # Afficher un apercu
    print("\nApercu des donnees nettoyees:")
    df_final.show(10)

    # Fermer Spark
    spark.stop()


if __name__ == "__main__":
    main()

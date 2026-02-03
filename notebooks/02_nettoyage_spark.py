# ===
# Etape 1.2 : Pipeline de nettoyage Spark
# ===

# Competence evaluee : C2.2 - Traiter des donnees structurees avec un langage de programmation**

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

PROJECT_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = PROJECT_DIR / "data_ecf"
OUTPUT_DIR = PROJECT_DIR / "output"
LOG_DIR = OUTPUT_DIR / "logs"
LOG_PATH = LOG_DIR / "02_nattoyage_spark.log"
CONSOMMATIONS_PATH = DATA_DIR / "consommations_raw.csv"
BATIMENTS_PATH = DATA_DIR / "batiments.csv"


def set_up_logger(
    name: str, logger_file_path: Path = None, level=logging.INFO
) -> logging.Logger:
    """Set up a logger.

    Args:
        name (str): Name of the logger.
        log_path (Path, optional): Path of the logger file. Defaults to None.
        level (_type_, optional): Level of the logger. Defaults to logging.INFO.

    Returns:
        logging.Logger: A configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter(
        "{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if logger_file_path:
        file_handler = logging.FileHandler(
            logger_file_path, mode="wt", encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


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
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = set_up_logger("02_nettoyage_spark", LOG_PATH)

    spark = create_spark_session()
    logger.info(f"Spark version: {spark.version}")

    # Enregistrer les UDFs
    parse_timestamp_udf = F.udf(parse_multi_format_timestamp, TimestampType())
    clean_value_udf = F.udf(clean_value, DoubleType())

    # Charger les donnees brutes
    logger.info("[1/6] Chargement des donnees brutes...")
    df_raw = spark.read.option("header", "true").csv(CONSOMMATIONS_PATH.as_posix())

    initial_count = df_raw.count()
    logger.info(f"  Lignes en entree: {initial_count:,}")

    df_batiments = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(BATIMENTS_PATH.as_posix())
    )

    # Parser les timestamps multi-formats
    logger.info("[2/6] Parsing des timestamps multi-formats...")
    df_with_timestamp = df_raw.withColumn(
        "timestamp", parse_timestamp_udf(F.col("timestamp"))
    )

    # Filtrer les timestamps invalides
    invalid_timestamps = df_with_timestamp.filter(F.col("timestamp").isNull()).count()
    df_with_timestamp = df_with_timestamp.filter(F.col("timestamp").isNotNull())
    logger.info(f"  Timestamps invalides supprimes: {invalid_timestamps:,}")

    # Convertir les valeurs avec virgule decimale en float
    logger.info("[3/6] Conversion des valeurs numeriques...")
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
    logger.info(f"  Valeurs non numeriques supprimees: {invalid_values:,}")

    # Filtrer les valeurs negatives et les outliers (>10000)
    logger.info("[4/6] Suppression des valeurs aberrantes...")
    negative_count = df_with_consommations.filter(F.col("consommation") < 0).count()
    outlier_count = df_with_consommations.filter(F.col("consommation") > 10_000).count()

    df_clean = df_with_consommations.filter(
        (F.col("consommation") >= 0) & (F.col("consommation") <= 10_000)
    )
    logger.info(f"  Valeurs negatives supprimees: {negative_count:,}")
    logger.info(f"  Outliers (> 10 000) supprimes: {outlier_count:,}")

    # Dedupliquer sur (batiment_id, timestamp, type_energie)
    logger.info("[5/6] Deduplication...")
    before_dedup = df_clean.count()
    df_dedup = df_clean.dropDuplicates(["batiment_id", "timestamp", "type_energie"])
    after_dedup = df_dedup.count()
    duplicates_removed = before_dedup - after_dedup
    logger.info(f"  Doublons supprimes: {duplicates_removed:,}")

    # Calculer les moyennes horaires par bâtiment et type d'énergie
    logger.info("[6/6] Agregation horaire et sauvegarde...")

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
    df_hourly_str = "\n".join([str(row) for row in df_hourly.collect()[:10]])
    logger.info(f"Consommations horaires moyennes par batiment :\n{df_hourly_str}")

    #   Consommations journalieres par batiment et type d'energie
    df_daily = df_with_time.groupBy("batiment_id", "type_energie", "date").agg(
        F.round(F.mean("consommation"), 2).alias("consommation_mean_day_bat_energy"),
        F.round(F.min("consommation"), 2).alias("consommation_min_day_bat_energy"),
        F.round(F.max("consommation"), 2).alias("consommation_max_day_bat_energy"),
    )
    df_daily_str = "\n".join([str(row) for row in df_daily.collect()[:10]])
    logger.info(
        f"Consommations journalieres par batiment et type d'energie :\n{df_daily_str}"
    )

    # Joindre avec les informations des batiments
    df_join = df_with_time.join(df_batiments, on="batiment_id", how="left")

    #   Consommations mensuelles par commune
    df_monthly = df_join.groupBy("commune", "year", "month").agg(
        F.round(F.mean("consommation"), 2).alias("consommation_mean_month_com"),
        F.round(F.min("consommation"), 2).alias("consommation_min_month_com"),
        F.round(F.max("consommation"), 2).alias("consommation_max_month_com"),
    )
    df_monthly_str = "\n".join([str(row) for row in df_monthly.collect()[:10]])
    logger.info(f"Consommations mensuelles par commune :\n{df_monthly_str}")

    # Sauvegarder en Parquet partitionne par date et type d'energie
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_with_time.write.mode("overwrite").partitionBy(["date", "type_energie"]).parquet(
        "/output/consommation_clean"
    )

    final_count = df_with_time.count()

    # Rapport final
    logger.info("RAPPORT DE NETTOYAGE")
    logger.info(f"Lignes en entree:              {initial_count:>12,}")
    logger.info(f"Timestamps invalides:          {invalid_timestamps:>12,}")
    logger.info(f"Valeurs non numeriques:        {invalid_values:>12,}")
    logger.info(f"Valeurs negatives:             {negative_count:>12,}")
    logger.info(f"Outliers (> 10 000):           {outlier_count:>12,}")
    logger.info(f"Doublons:                      {duplicates_removed:>12,}")
    total_removed = (
        invalid_timestamps
        + invalid_values
        + negative_count
        + outlier_count
        + duplicates_removed
    )
    logger.info(f"Total lignes supprimees:       {total_removed:>12,}")
    logger.info(f"Lignes apres agregation:       {final_count:>12,}")
    logger.info(f"Fichiers Parquet sauvegardes dans: {OUTPUT_DIR}")

    # Afficher un apercu
    logger.info("Apercu des donnees nettoyees:")
    df_with_time.show(10)

    # Fermer Spark
    spark.stop()


if __name__ == "__main__":
    main()

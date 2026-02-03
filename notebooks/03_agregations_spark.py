import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = PROJECT_DIR / "data_ecf"
OUTPUT_DIR = PROJECT_DIR / "output"
LOG_DIR = OUTPUT_DIR / "logs"
LOG_PATH = LOG_DIR / "03_aggregation_spark.log"
CONSOMMATIONS_PATH = OUTPUT_DIR / "consommation_clean"
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
        builder.appName("Test")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = set_up_logger("03_aggregation_spark", LOG_PATH)

    spark = create_spark_session()
    logger.info(f"Spark version: {spark.version}")

    # Charger les donnees brutes
    logger.info("\n[1/4] Chargement des donnees ...")
    df_consommations = spark.read.parquet(CONSOMMATIONS_PATH.as_posix())

    df_batiments = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(BATIMENTS_PATH.as_posix())
    )

    # Jointure
    logger.info("\n[2/4] Jointure ...")
    df_join = df_consommations.join(df_batiments, on="batiment_id", how="left")

    # Calculer l'intensite energetique (kWh/m2)
    logger.info("\n[3/4] Calcule intensité énergétique ...")
    df_with_intensity = df_join.withColumn(
        "intensite_energetique",
        F.col("consommation") / F.col("surface_m2"),
    )

    logger.info("\n[4/4] Sauvegarde Parquet ...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_with_intensity.write.mode("overwrite").partitionBy(["type_energie"]).parquet(
        "/output/consommations_agregees"
    )

    spark.stop()


if __name__ == "__main__":
    main()

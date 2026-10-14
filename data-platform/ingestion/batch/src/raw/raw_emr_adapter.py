import argparse
import json
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
from pyspark.sql import SparkSession

from common_libs.schema_registry import get_entity_config
from common_libs.utils import format_utc_timestamp, parse_utc_timestamp
from common_libs.watermark_store import SsmWatermarkStore
from raw_ingestion_core import run_raw_ingestion  # Core reutilizable; el adapter solo prepara parámetros


# Parámetros del job (EMR: se pasan por línea de comandos; en Glue venían de getResolvedOptions)
ARGUMENTS = [
    "target_entity",         # books, customers, orders, order_items
    "source_schema",         # esquema de PostgreSQL
    "source_system",         # identificador del sistema de origen
    "s3_target_path",        # ruta base en S3
    "ssm_watermark_prefix",  # prefijo SSM de los watermarks
    "initial_watermark",     # fecha inicial si no hay watermark
    "run_id",                # id único de la ejecución
    "jdbc_url",
    "db_secret_name",        # secreto con usuario y contraseña
]


def log_event(event: str, **fields: Any) -> None:
    """Log estructurado en JSON (a CloudWatch si la salida se publica allí)."""
    payload = {
        "event": event,
        "timestamp": format_utc_timestamp(datetime.now(timezone.utc)),
        **fields,
    }
    print(json.dumps(payload, sort_keys=True, default=str))


def read_postgres_query(spark, *, jdbc_url, user, password, sample_query):
    """Ejecuta una query por JDBC; el JAR del driver debe estar en Spark."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("query", sample_query)
        .load()
    )


def read_database_upper_bound(spark, *, jdbc_url, user, password) -> datetime:
    """Hora actual de PostgreSQL: límite superior del intervalo."""
    df = read_postgres_query(
        spark, jdbc_url=jdbc_url, user=user, password=password,
        sample_query="SELECT CURRENT_TIMESTAMP AS current_run_upper_bound",
    )
    rows = df.limit(2).collect()
    if len(rows) != 1 or rows[0]["current_run_upper_bound"] is None:
        raise RuntimeError("PostgreSQL did not return exactly one upper-bound timestamp")
    return parse_utc_timestamp(rows[0]["current_run_upper_bound"], assume_naive_utc=True)


def run_job(args: Dict[str, str]) -> None:
    # EMR: SparkSession directo (en Glue era GlueContext)
    spark = SparkSession.builder.appName("raw_ingestion").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # El rol IAM del cluster necesita permiso sobre el secreto
    secret = json.loads(
        boto3.client("secretsmanager")
        .get_secret_value(SecretId=args["db_secret_name"])["SecretString"]
    )
    user, password = secret["username"], secret["password"]
    jdbc_url = args["jdbc_url"]

    # Intervalo: watermark anterior -> hora actual de PostgreSQL
    entity_config = get_entity_config(args["target_entity"])
    watermark_store = SsmWatermarkStore(boto3.client("ssm"), args["ssm_watermark_prefix"])
    watermark_state = watermark_store.read(entity_config.name, args["initial_watermark"])
    upper_bound = read_database_upper_bound(
        spark, jdbc_url=jdbc_url, user=user, password=password
    )
    if watermark_state.value > upper_bound:
        raise RuntimeError("Stored watermark is later than PostgreSQL CURRENT_TIMESTAMP")

    # initial: no había watermark guardado
    ingestion_mode = "incremental" if watermark_state.exists else "initial"
    ingested_at = datetime.now(timezone.utc)
    log_event(
        "raw_ingestion_started",
        entity=entity_config.name,
        ingestion_mode=ingestion_mode,
        run_id=args["run_id"],
        watermark_from=format_utc_timestamp(watermark_state.value),
        watermark_to=format_utc_timestamp(upper_bound),
    )

    # El Core pasa la query ya construida; source_table y entity_name no se usan aquí
    def source_reader(sample_query: str, source_table: str, entity_name: str) -> Any:
        return read_postgres_query(
            spark, jdbc_url=jdbc_url, user=user, password=password,
            sample_query=sample_query,
        )

    # Core: lee, normaliza, añade metadatos y escribe Parquet en S3
    result = run_raw_ingestion(
        source_reader=source_reader,
        target_entity=entity_config.name,
        source_schema=args["source_schema"],
        source_system=args["source_system"],
        s3_target_path=args["s3_target_path"],
        run_id=args["run_id"],
        ingestion_mode=ingestion_mode,
        ingested_at=ingested_at,
        lower_bound=watermark_state.value,
        upper_bound=upper_bound,
    )

    # Solo se avanza si el Core terminó bien
    watermark_updated = watermark_store.advance(
        entity_config.name, watermark_state, upper_bound
    )
    log_event(
        "raw_ingestion_completed",
        entity=result.entity_name,
        row_count=result.row_count,
        run_id=result.run_id,
        target_path=result.target_path,
        watermark_updated=watermark_updated,
        watermark_to=format_utc_timestamp(result.upper_bound),
    )
    spark.stop()  # TODO: no se ejecuta si hay excepción; valorar try/finally


def main() -> None:
    parser = argparse.ArgumentParser()
    for name in ARGUMENTS:
        parser.add_argument(f"--{name}", required=True)
    run_job(vars(parser.parse_args()))


if __name__ == "__main__":
    main()

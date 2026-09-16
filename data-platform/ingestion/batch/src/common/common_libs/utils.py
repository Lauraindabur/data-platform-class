import re
from datetime import datetime, timezone
from typing import Any, Union
from urllib.parse import urlparse

from common_libs.schema_registry import EntityConfig


TimestampValue = Union[str, datetime]

_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def validate_identifier(value: str, label: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def validate_run_id(run_id: str) -> str:
    if not _RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            "run_id must contain only letters, numbers, '.', '_' or '-', "
            "must start with a letter or number, and must have at most 128 characters"
        )
    return run_id


def validate_source_system(source_system: str) -> str:
    normalized = source_system.strip()
    if not normalized or len(normalized) > 128:
        raise ValueError("source_system must contain between 1 and 128 characters")
    return normalized


def validate_s3_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return uri.rstrip("/")


def parse_utc_timestamp(
    value: TimestampValue,
    *,
    assume_naive_utc: bool = False,
) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Invalid ISO-8601 timestamp: {value!r}") from exc
    else:
        raise TypeError(f"Unsupported timestamp value: {type(value).__name__}")

    if parsed.tzinfo is None:
        if not assume_naive_utc:
            raise ValueError(f"Timestamp must include a timezone: {value!r}")
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def format_utc_timestamp(value: TimestampValue) -> str:
    parsed = parse_utc_timestamp(value, assume_naive_utc=True)
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def format_postgres_timestamp(value: TimestampValue) -> str:
    parsed = parse_utc_timestamp(value, assume_naive_utc=True)
    return parsed.isoformat(sep=" ", timespec="microseconds")


def qualified_table_name(source_schema: str, source_table: str) -> str:
    schema = validate_identifier(source_schema, "source_schema")
    table = validate_identifier(source_table, "source_table")
    return f"{schema}.{table}"


def build_incremental_query(
    entity_config: EntityConfig,
    source_schema: str,
    lower_bound: TimestampValue,
    upper_bound: TimestampValue,
) -> str:
    lower = parse_utc_timestamp(lower_bound, assume_naive_utc=True)
    upper = parse_utc_timestamp(upper_bound, assume_naive_utc=True)
    if lower > upper:
        raise ValueError("The lower watermark cannot be later than the upper bound")

    schema = validate_identifier(source_schema, "source_schema")
    table = validate_identifier(entity_config.source_table, "source_table")
    watermark_column = validate_identifier(
        entity_config.watermark_column,
        "watermark_column",
    )
    selected_columns = ", ".join(
        f'"{validate_identifier(column.name, "column")}"'
        for column in entity_config.columns
    )

    lower_literal = format_postgres_timestamp(lower)
    upper_literal = format_postgres_timestamp(upper)

    return (
        f'SELECT {selected_columns} '
        f'FROM "{schema}"."{table}" '
        f'WHERE "{watermark_column}" > TIMESTAMPTZ \'{lower_literal}\' '
        f'AND "{watermark_column}" <= TIMESTAMPTZ \'{upper_literal}\''
    )


def normalize_source_dataframe(dataframe: Any, entity_config: EntityConfig) -> Any:
    from pyspark.sql import functions as F

    projections = [
        F.col(column.name).cast(column.spark_type).alias(column.name)
        for column in entity_config.columns
    ]
    return dataframe.select(*projections)


def add_ingestion_metadata(
    dataframe: Any,
    entity_config: EntityConfig,
    *,
    source_schema: str,
    source_system: str,
    run_id: str,
    ingestion_mode: str,
    ingested_at: TimestampValue,
    lower_bound: TimestampValue,
    upper_bound: TimestampValue,
) -> Any:
    from pyspark.sql import functions as F

    if ingestion_mode not in {"initial", "incremental"}:
        raise ValueError(f"Invalid ingestion_mode: {ingestion_mode!r}")

    validate_run_id(run_id)
    source_system = validate_source_system(source_system)
    source_table = qualified_table_name(source_schema, entity_config.source_table)

    ingested_at_literal = format_postgres_timestamp(ingested_at)
    lower_literal = format_postgres_timestamp(lower_bound)
    upper_literal = format_postgres_timestamp(upper_bound)
    source_columns = [F.col(column.name) for column in entity_config.columns]
    record_json = F.to_json(
        F.struct(*source_columns),
        options={"ignoreNullFields": "false"},
    )

    enriched = (
        dataframe.withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_ingested_at", F.lit(ingested_at_literal).cast("timestamp"))
        .withColumn("_ingestion_date", F.to_date(F.col("_ingested_at")))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingestion_mode", F.lit(ingestion_mode))
        .withColumn("_watermark_from", F.lit(lower_literal).cast("timestamp"))
        .withColumn("_watermark_to", F.lit(upper_literal).cast("timestamp"))
        .withColumn("_record_hash", F.sha2(record_json, 256))
    )

    metadata_columns = [
        "_source_system",
        "_source_table",
        "_ingested_at",
        "_ingestion_date",
        "_run_id",
        "_ingestion_mode",
        "_watermark_from",
        "_watermark_to",
        "_record_hash",
    ]
    ordered_columns = [column.name for column in entity_config.columns] + metadata_columns
    return enriched.select(*ordered_columns)


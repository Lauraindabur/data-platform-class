import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from common_libs.schema_registry import get_entity_config
from common_libs.utils import (
    format_utc_timestamp,
    parse_utc_timestamp,
    qualified_table_name,
)
from common_libs.watermark_store import SsmWatermarkStore
from raw_ingestion_core import run_raw_ingestion


REQUIRED_ARGUMENTS = [
    "JOB_NAME",
    "connection_name",
    "target_entity",
    "source_schema",
    "source_system",
    "s3_target_path",
    "ssm_watermark_prefix",
    "initial_watermark",
    "run_id",
]


def log_event(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "timestamp": format_utc_timestamp(datetime.now(timezone.utc)),
        **fields,
    }
    print(json.dumps(payload, sort_keys=True, default=str))


def read_postgres_query(
    glue_context: GlueContext,
    *,
    connection_name: str,
    dbtable: str,
    sample_query: str,
    transformation_context: str,
) -> Any:
    source_dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": connection_name,
            "dbtable": dbtable,
            "sampleQuery": sample_query,
        },
        transformation_ctx=transformation_context,
    )
    return source_dyf.toDF()


def read_database_upper_bound(
    glue_context: GlueContext,
    *,
    connection_name: str,
    dbtable: str,
    target_entity: str,
) -> datetime:
    upper_bound_df = read_postgres_query(
        glue_context,
        connection_name=connection_name,
        dbtable=dbtable,
        sample_query="SELECT CURRENT_TIMESTAMP AS current_run_upper_bound",
        transformation_context=f"{target_entity}_upper_bound",
    )
    rows = upper_bound_df.limit(2).collect()
    if len(rows) != 1 or rows[0]["current_run_upper_bound"] is None:
        raise RuntimeError("PostgreSQL did not return exactly one upper-bound timestamp")
    return parse_utc_timestamp(
        rows[0]["current_run_upper_bound"],
        assume_naive_utc=True,
    )


def run_job(args: Dict[str, str]) -> None:
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    entity_config = get_entity_config(args["target_entity"])
    dbtable = qualified_table_name(args["source_schema"], entity_config.source_table)
    watermark_store = SsmWatermarkStore(
        boto3.client("ssm"),
        args["ssm_watermark_prefix"],
    )
    watermark_state = watermark_store.read(
        entity_config.name,
        args["initial_watermark"],
    )
    upper_bound = read_database_upper_bound(
        glue_context,
        connection_name=args["connection_name"],
        dbtable=dbtable,
        target_entity=entity_config.name,
    )
    if watermark_state.value > upper_bound:
        raise RuntimeError(
            "Stored watermark is later than PostgreSQL CURRENT_TIMESTAMP; "
            "check clocks and watermark configuration"
        )

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

    def source_reader(sample_query: str, source_table: str, entity_name: str) -> Any:
        return read_postgres_query(
            glue_context,
            connection_name=args["connection_name"],
            dbtable=source_table,
            sample_query=sample_query,
            transformation_context=f"{entity_name}_incremental_source",
        )

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

    watermark_updated = watermark_store.advance(
        entity_config.name,
        watermark_state,
        upper_bound,
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
    job.commit()


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGUMENTS)
    run_job(args)


if __name__ == "__main__":
    main()


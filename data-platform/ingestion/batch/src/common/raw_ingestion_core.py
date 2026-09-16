from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from common_libs.schema_registry import get_entity_config
from common_libs.utils import (
    add_ingestion_metadata,
    build_incremental_query,
    normalize_source_dataframe,
    qualified_table_name,
    validate_run_id,
    validate_s3_uri,
    validate_source_system,
)


SourceReader = Callable[[str, str, str], Any]


@dataclass(frozen=True)
class IngestionResult:
    entity_name: str
    row_count: int
    target_path: str
    lower_bound: datetime
    upper_bound: datetime
    run_id: str


def run_raw_ingestion(
    *,
    source_reader: SourceReader,
    target_entity: str,
    source_schema: str,
    source_system: str,
    s3_target_path: str,
    run_id: str,
    ingestion_mode: str,
    ingested_at: datetime,
    lower_bound: datetime,
    upper_bound: datetime,
) -> IngestionResult:
    """Extract one bounded interval and append it to the Raw zone."""
    from pyspark import StorageLevel

    entity_config = get_entity_config(target_entity)
    run_id = validate_run_id(run_id)
    source_system = validate_source_system(source_system)
    raw_base_path = validate_s3_uri(s3_target_path)
    source_table = qualified_table_name(source_schema, entity_config.source_table)
    sample_query = build_incremental_query(
        entity_config,
        source_schema,
        lower_bound,
        upper_bound,
    )

    source_df = source_reader(sample_query, source_table, entity_config.name)
    normalized_df = normalize_source_dataframe(source_df, entity_config)
    enriched_df = add_ingestion_metadata(
        normalized_df,
        entity_config,
        source_schema=source_schema,
        source_system=source_system,
        run_id=run_id,
        ingestion_mode=ingestion_mode,
        ingested_at=ingested_at,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )

    target_path = f"{raw_base_path}/{entity_config.name}/"
    cached_df = enriched_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        row_count = cached_df.count()
        if row_count > 0:
            (
                cached_df.write.mode("append")
                .format("parquet")
                .partitionBy("_ingestion_date", "_run_id")
                .save(target_path)
            )
    finally:
        cached_df.unpersist()

    return IngestionResult(
        entity_name=entity_config.name,
        row_count=row_count,
        target_path=target_path,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        run_id=run_id,
    )


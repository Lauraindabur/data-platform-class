from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    spark_type: str


@dataclass(frozen=True)
class EntityConfig:
    name: str
    source_table: str
    primary_key: Tuple[str, ...]
    watermark_column: str
    columns: Tuple[ColumnSpec, ...]


ENTITY_CONFIGS: Dict[str, EntityConfig] = {
    "books": EntityConfig(
        name="books",
        source_table="books",
        primary_key=("book_id",),
        watermark_column="updated_at",
        columns=(
            ColumnSpec("book_id", "string"),
            ColumnSpec("isbn", "string"),
            ColumnSpec("title", "string"),
            ColumnSpec("author", "string"),
            ColumnSpec("category", "string"),
            ColumnSpec("publisher", "string"),
            ColumnSpec("publication_year", "int"),
            ColumnSpec("price", "decimal(10,2)"),
            ColumnSpec("stock", "int"),
            ColumnSpec("description", "string"),
            ColumnSpec("created_at", "timestamp"),
            ColumnSpec("updated_at", "timestamp"),
            ColumnSpec("deleted_at", "timestamp"),
        ),
    ),
    "customers": EntityConfig(
        name="customers",
        source_table="customers",
        primary_key=("customer_id",),
        watermark_column="updated_at",
        columns=(
            ColumnSpec("customer_id", "string"),
            ColumnSpec("first_name", "string"),
            ColumnSpec("last_name", "string"),
            ColumnSpec("email", "string"),
            ColumnSpec("phone", "string"),
            ColumnSpec("city", "string"),
            ColumnSpec("country", "string"),
            ColumnSpec("created_at", "timestamp"),
            ColumnSpec("updated_at", "timestamp"),
            ColumnSpec("deleted_at", "timestamp"),
        ),
    ),
    "orders": EntityConfig(
        name="orders",
        source_table="orders",
        primary_key=("order_id",),
        watermark_column="updated_at",
        columns=(
            ColumnSpec("order_id", "string"),
            ColumnSpec("customer_id", "string"),
            ColumnSpec("order_status", "string"),
            ColumnSpec("total_amount", "decimal(12,2)"),
            ColumnSpec("order_timestamp", "timestamp"),
            ColumnSpec("created_at", "timestamp"),
            ColumnSpec("updated_at", "timestamp"),
            ColumnSpec("deleted_at", "timestamp"),
        ),
    ),
    "order_items": EntityConfig(
        name="order_items",
        source_table="order_items",
        primary_key=("order_item_id",),
        watermark_column="updated_at",
        columns=(
            ColumnSpec("order_item_id", "string"),
            ColumnSpec("order_id", "string"),
            ColumnSpec("book_id", "string"),
            ColumnSpec("quantity", "int"),
            ColumnSpec("unit_price", "decimal(10,2)"),
            ColumnSpec("line_total", "decimal(12,2)"),
            ColumnSpec("created_at", "timestamp"),
            ColumnSpec("updated_at", "timestamp"),
        ),
    ),
}


def get_entity_config(entity_name: str) -> EntityConfig:
    normalized_name = entity_name.strip().lower()
    try:
        return ENTITY_CONFIGS[normalized_name]
    except KeyError as exc:
        supported = ", ".join(sorted(ENTITY_CONFIGS))
        raise ValueError(
            f"Unsupported entity '{entity_name}'. Supported entities: {supported}"
        ) from exc


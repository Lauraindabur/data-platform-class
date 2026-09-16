import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from common_libs.utils import format_utc_timestamp, parse_utc_timestamp


_PARAMETER_PREFIX_PATTERN = re.compile(r"^/[A-Za-z0-9_.\-/]+$")


class WatermarkConflictError(RuntimeError):
    """Raised when another execution changed the watermark unexpectedly."""


@dataclass(frozen=True)
class WatermarkState:
    parameter_name: str
    value: datetime
    exists: bool


class SsmWatermarkStore:
    def __init__(self, ssm_client: Any, parameter_prefix: str) -> None:
        normalized_prefix = parameter_prefix.rstrip("/")
        if not _PARAMETER_PREFIX_PATTERN.fullmatch(normalized_prefix):
            raise ValueError(f"Invalid SSM parameter prefix: {parameter_prefix!r}")
        self._client = ssm_client
        self._parameter_prefix = normalized_prefix

    def parameter_name(self, entity_name: str) -> str:
        return f"{self._parameter_prefix}/{entity_name}"

    def read(self, entity_name: str, initial_watermark: str) -> WatermarkState:
        name = self.parameter_name(entity_name)
        try:
            response = self._client.get_parameter(Name=name, WithDecryption=False)
        except Exception as exc:
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code != "ParameterNotFound":
                raise
            return WatermarkState(
                parameter_name=name,
                value=parse_utc_timestamp(initial_watermark),
                exists=False,
            )

        return WatermarkState(
            parameter_name=name,
            value=parse_utc_timestamp(response["Parameter"]["Value"]),
            exists=True,
        )

    def advance(
        self,
        entity_name: str,
        expected_state: WatermarkState,
        new_watermark: datetime,
    ) -> bool:
        if expected_state.parameter_name != self.parameter_name(entity_name):
            raise ValueError("The expected watermark state belongs to another entity")

        current_state = self.read(
            entity_name,
            format_utc_timestamp(expected_state.value),
        )
        if (
            current_state.exists != expected_state.exists
            or current_state.value != expected_state.value
        ):
            raise WatermarkConflictError(
                f"Watermark changed concurrently for {current_state.parameter_name}"
            )

        normalized_new_watermark = parse_utc_timestamp(
            new_watermark,
            assume_naive_utc=True,
        )
        if normalized_new_watermark < current_state.value:
            raise ValueError("The watermark cannot move backwards")
        if current_state.exists and normalized_new_watermark == current_state.value:
            return False

        self._client.put_parameter(
            Name=current_state.parameter_name,
            Value=format_utc_timestamp(normalized_new_watermark),
            Type="String",
            Overwrite=True,
            Description=f"BookStore N1 ingestion watermark for {entity_name}",
        )
        return True


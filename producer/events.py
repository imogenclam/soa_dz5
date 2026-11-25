import json
import struct
from datetime import UTC, date, datetime
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Annotated, Self
from uuid import UUID

from fastavro import parse_schema, schemaless_writer
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator


SCHEMA_FILE = Path(__file__).resolve().parents[1] / "schemas" / "movie-event.avsc"
SCHEMA_TEXT = SCHEMA_FILE.read_text(encoding="utf-8")
AVRO_SCHEMA = parse_schema(json.loads(SCHEMA_TEXT))
Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Progress = Annotated[int, Field(strict=True, ge=0, le=2**31 - 1)]


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    user_id: Identifier
    movie_id: Identifier
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: Identifier
    progress_seconds: Progress | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp(cls, value):
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("укажи время с часовым поясом")
        try:
            utc = value.astimezone(UTC)
        except OverflowError as error:
            raise ValueError("время выходит за диапазон utc") from error
        if not date(1970, 1, 1) <= utc.date() <= date(2149, 6, 6):
            raise ValueError("дата выходит за диапазон дневных метрик")
        return utc.replace(microsecond=utc.microsecond // 1000 * 1000)

    @model_validator(mode="after")
    def require_watch_progress(self) -> Self:
        if self.event_type.startswith("VIEW_") and self.progress_seconds is None:
            raise ValueError("для просмотра нужен progress_seconds")
        return self


def encode_event(event: MovieEvent, schema_id: int) -> bytes:
    out = BytesIO()
    out.write(struct.pack(">BI", 0, schema_id))
    schemaless_writer(out, AVRO_SCHEMA, event.model_dump(), strict=True)
    return out.getvalue()

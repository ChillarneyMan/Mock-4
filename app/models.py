from datetime import datetime
from typing import Any
from pydantic import BaseModel, field_validator, model_validator


class Event(BaseModel):
    event_type: str
    source: str
    timestamp: datetime
    payload: dict[str, Any]

    @field_validator("event_type", "source")
    @classmethod
    def must_be_non_empty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} must be a non-empty string")
        return v

    @field_validator("payload", mode="before")
    @classmethod
    def payload_must_be_object(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            raise ValueError(
                f"payload must be a JSON object (got {type(v).__name__})"
            )
        return v


class EventAccepted(BaseModel):
    status: str = "accepted"
    buffered: bool = True


class StatsResponse(BaseModel):
    event_type: str
    date: str
    count: int

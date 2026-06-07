from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import Any, TextIO


def log_event(service: str, event: str, *, level: str = "info", **fields: Any) -> None:
    stream = sys.stderr if level in {"warning", "error"} else sys.stdout
    write_event(stream, service, event, level=level, **fields)


def log_metric(
    service: str,
    metric_name: str,
    metric_value: int | float,
    *,
    metric_unit: str = "count",
    level: str = "info",
    **fields: Any,
) -> None:
    log_event(
        service,
        "metric_observed",
        level=level,
        metric_name=metric_name,
        metric_value=metric_value,
        metric_unit=metric_unit,
        **fields,
    )


def write_event(
    stream: TextIO,
    service: str,
    event: str,
    *,
    level: str = "info",
    **fields: Any,
) -> None:
    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": level,
        "service": service,
        "event": event,
        **fields,
    }
    print(json.dumps(record, ensure_ascii=False, sort_keys=True), file=stream, flush=True)

"""
Audit logging for VOOI API calls.
Every request/response is persisted to:
  1. vooi_api_calls table (all calls)
  2. vooi_errors table (errors only)
  3. vooi-raw.log file (raw NDJSON for debugging)
"""
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

import aiofiles
import structlog

from bot.config import settings
from bot.db import session_scope
from bot.models import VooiApiCall, VooiError

log = structlog.get_logger(__name__)


async def _ensure_log_dir() -> None:
    """Ensure log directory exists."""
    log_dir = os.path.dirname(settings.vooi_raw_log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)


async def write_raw_log(entry: dict[str, Any]) -> None:
    """Append a NDJSON line to vooi-raw.log."""
    try:
        await _ensure_log_dir()
        line = json.dumps(entry, default=str) + "\n"
        async with aiofiles.open(settings.vooi_raw_log_path, mode="a", encoding="utf-8") as f:
            await f.write(line)
    except Exception as e:
        log.warning("vooi_raw_log_write_failed", error=str(e))


async def insert_api_call_log(
    correlation_id: str,
    method: str,
    path: str,
    request_body: Optional[str],
    response_status: Optional[int],
    response_body: Optional[str],
    duration_ms: Optional[int],
) -> None:
    """Persist API call to vooi_api_calls table and raw log."""
    # Write to DB
    try:
        async with session_scope() as session:
            call = VooiApiCall(
                correlation_id=correlation_id,
                method=method,
                path=path,
                request_body=request_body,
                response_status=response_status,
                response_body=response_body,
                duration_ms=duration_ms,
            )
            session.add(call)
    except Exception as e:
        log.error("vooi_audit_db_write_failed", error=str(e), correlation_id=correlation_id)

    # Write to raw log
    await write_raw_log(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "correlation_id": correlation_id,
            "type": "api_call",
            "method": method,
            "path": path,
            "status": response_status,
            "duration_ms": duration_ms,
        }
    )


async def insert_error_log(
    correlation_id: str,
    method: str,
    path: str,
    request_body: Optional[str],
    error_kind: str,
    error_detail: Optional[str],
    response_status: Optional[int],
    response_body: Optional[str],
    duration_ms: Optional[int],
) -> None:
    """Persist error to vooi_errors table and raw log."""
    # Write to DB
    try:
        async with session_scope() as session:
            error = VooiError(
                correlation_id=correlation_id,
                method=method,
                path=path,
                request_body=request_body,
                error_kind=error_kind,
                error_detail=error_detail,
                response_status=response_status,
                response_body=response_body,
                duration_ms=duration_ms,
                reported=False,
            )
            session.add(error)
    except Exception as e:
        log.error(
            "vooi_error_db_write_failed",
            error=str(e),
            correlation_id=correlation_id,
            error_kind=error_kind,
        )

    # Write to raw log
    await write_raw_log(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "correlation_id": correlation_id,
            "type": "api_error",
            "method": method,
            "path": path,
            "error_kind": error_kind,
            "error_detail": error_detail,
            "status": response_status,
            "duration_ms": duration_ms,
        }
    )

    log.error(
        "vooi_api_error",
        correlation_id=correlation_id,
        method=method,
        path=path,
        error_kind=error_kind,
        error_detail=error_detail,
        status=response_status,
    )

"""
Audit logging for VOOI API calls.
Every request/response is persisted to:
  1. vooi_api_calls table (all calls)
  2. vooi_errors table (errors only)
  3. vooi-raw.log file (raw NDJSON for debugging)
  4. aster-debug.log file (verbose NDJSON for aster-only calls: full
     request/response headers + bodies + per-attempt entries) — for
     diagnosing intermittent aster 401 "Invalid credentials" bursts.
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


# -----------------------------------------------------------------------------
# Aster detection — used to route verbose logging.
# -----------------------------------------------------------------------------
def _safe_json_load(text: Optional[str]) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def is_aster_call(
    request_body: Optional[str],
    params: Optional[dict[str, Any]],
    response_body: Optional[str] = None,
) -> bool:
    """True if the call targets the aster exchange.

    Detection is best-effort across the three places "aster" can appear:
      • JSON body field "exchange" (POST /exchange/orders, leverage, etc.)
      • Query string field "exchanges" (GET /exchange/quotes, /open-orders, ...)
      • Response body — covers cases where VOOI echoes the exchange even when
        we didn't pin it (rare; defensive).
    """
    body = _safe_json_load(request_body)
    if isinstance(body, dict):
        exch = str(body.get("exchange", "")).lower()
        if exch == "aster":
            return True

    if params:
        exchanges = str(params.get("exchanges", "")).lower()
        if "aster" in exchanges:
            return True
        if str(params.get("exchange", "")).lower() == "aster":
            return True

    if response_body and '"exchange":"aster"' in response_body:
        return True

    return False


async def _ensure_log_dir() -> None:
    """Ensure log directory exists."""
    log_dir = os.path.dirname(settings.vooi_raw_log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    aster_dir = os.path.dirname(settings.aster_debug_log_path)
    if aster_dir:
        os.makedirs(aster_dir, exist_ok=True)


async def write_raw_log(entry: dict[str, Any]) -> None:
    """Append a NDJSON line to vooi-raw.log."""
    try:
        await _ensure_log_dir()
        line = json.dumps(entry, default=str) + "\n"
        async with aiofiles.open(settings.vooi_raw_log_path, mode="a", encoding="utf-8") as f:
            await f.write(line)
    except Exception as e:
        log.warning("vooi_raw_log_write_failed", error=str(e))


async def write_aster_debug_log(entry: dict[str, Any]) -> None:
    """Append a verbose NDJSON line to aster-debug.log.

    Use for any phase (request_sent / response_received / error) of a call
    targeting the aster exchange. Designed to be `tail -F`'d during incident
    triage and grep'd by correlation_id / time window.
    """
    try:
        await _ensure_log_dir()
        line = json.dumps(entry, default=str) + "\n"
        async with aiofiles.open(
            settings.aster_debug_log_path, mode="a", encoding="utf-8"
        ) as f:
            await f.write(line)
    except Exception as e:
        log.warning("aster_debug_log_write_failed", error=str(e))


async def log_aster_request_sent(
    correlation_id: str,
    method: str,
    url: str,
    request_headers: dict[str, str],
    request_body: Optional[str],
    params: Optional[dict[str, Any]],
    attempt: int,
) -> None:
    """Pre-flight entry: written BEFORE the network call. Lets us reconstruct
    the action sequence even if the response never arrives (timeout, crash).
    """
    if not is_aster_call(request_body, params):
        return
    now = datetime.now(timezone.utc)
    await write_aster_debug_log(
        {
            "ts": now.isoformat(),
            "ts_epoch_ms": int(now.timestamp() * 1000),
            "correlation_id": correlation_id,
            "phase": "request_sent",
            "method": method,
            "url": url,
            "params": params,
            "request_headers": request_headers,
            "request_body": request_body,
            "attempt": attempt,
        }
    )


async def insert_api_call_log(
    correlation_id: str,
    method: str,
    path: str,
    request_body: Optional[str],
    response_status: Optional[int],
    response_body: Optional[str],
    duration_ms: Optional[int],
    request_headers: Optional[dict[str, str]] = None,
    response_headers: Optional[dict[str, str]] = None,
    params: Optional[dict[str, Any]] = None,
    attempt: int = 1,
) -> None:
    """Persist API call to vooi_api_calls table and raw log."""
    # Write to DB (schema unchanged — headers stay in file logs only)
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

    now = datetime.now(timezone.utc)

    # Write to raw log (summary, all exchanges)
    await write_raw_log(
        {
            "ts": now.isoformat(),
            "correlation_id": correlation_id,
            "type": "api_call",
            "method": method,
            "path": path,
            "status": response_status,
            "duration_ms": duration_ms,
            "attempt": attempt,
        }
    )

    # Verbose aster log — full request + response + headers
    if is_aster_call(request_body, params, response_body):
        await write_aster_debug_log(
            {
                "ts": now.isoformat(),
                "ts_epoch_ms": int(now.timestamp() * 1000),
                "correlation_id": correlation_id,
                "phase": "response_received",
                "method": method,
                "path": path,
                "params": params,
                "request_headers": request_headers,
                "request_body": request_body,
                "response_status": response_status,
                "response_headers": response_headers,
                "response_body": response_body,
                "duration_ms": duration_ms,
                "attempt": attempt,
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
    request_headers: Optional[dict[str, str]] = None,
    response_headers: Optional[dict[str, str]] = None,
    params: Optional[dict[str, Any]] = None,
    attempt: int = 1,
) -> None:
    """Persist error to vooi_errors table and raw log."""
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

    now = datetime.now(timezone.utc)

    await write_raw_log(
        {
            "ts": now.isoformat(),
            "correlation_id": correlation_id,
            "type": "api_error",
            "method": method,
            "path": path,
            "error_kind": error_kind,
            "error_detail": error_detail,
            "status": response_status,
            "duration_ms": duration_ms,
            "attempt": attempt,
        }
    )

    aster = is_aster_call(request_body, params, response_body)

    # Verbose aster log — full request + response + headers for the failure
    if aster:
        await write_aster_debug_log(
            {
                "ts": now.isoformat(),
                "ts_epoch_ms": int(now.timestamp() * 1000),
                "correlation_id": correlation_id,
                "phase": "error",
                "method": method,
                "path": path,
                "params": params,
                "request_headers": request_headers,
                "request_body": request_body,
                "error_kind": error_kind,
                "error_detail": error_detail,
                "response_status": response_status,
                "response_headers": response_headers,
                "response_body": response_body,
                "duration_ms": duration_ms,
                "attempt": attempt,
            }
        )

    # Standard structured error (goes to bot.log + stdout)
    log.error(
        "vooi_api_error",
        correlation_id=correlation_id,
        method=method,
        path=path,
        error_kind=error_kind,
        error_detail=error_detail,
        status=response_status,
    )

    # Aster 401 — surface a loud warning with the diagnostic fields visible
    # right in the live console, so an operator does not have to query the
    # DB to see what just happened.
    if aster and response_status == 401:
        vooi_request_id = None
        if response_headers:
            # VOOI runs on fly.io behind CloudFlare; the request id we can
            # actually give to their support is `fly-request-id`. `cf-ray`
            # is the CloudFlare edge id (fallback). The x-* names are kept
            # in case VOOI ever starts emitting their own header.
            for h in ("fly-request-id", "cf-ray", "x-request-id",
                      "x-correlation-id", "x-amzn-requestid",
                      "x-vooi-request-id"):
                for k, v in response_headers.items():
                    if k.lower() == h:
                        vooi_request_id = f"{h}={v}"
                        break
                if vooi_request_id:
                    break

        log.warning(
            "aster_auth_failed",
            correlation_id=correlation_id,
            vooi_request_id=vooi_request_id,
            method=method,
            path=path,
            attempt=attempt,
            request_body=request_body,
            response_body=response_body,
            response_headers=response_headers,
            duration_ms=duration_ms,
        )

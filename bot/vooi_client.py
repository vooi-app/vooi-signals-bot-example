"""
VOOI Ultra API async HTTP client with:
- Full audit interceptor (every request logged to DB + raw file)
- correlation_id per request
- Authorization header redaction in logs
- Error kind taxonomy
- Retry with exponential backoff for 5xx / timeout
- Timing (duration_ms)
"""
import asyncio
import json
import time
import uuid
from typing import Any, Optional

import httpx
import structlog

from bot.config import settings
from bot.vooi_audit import (
    insert_api_call_log,
    insert_error_log,
    log_aster_request_sent,
)

log = structlog.get_logger(__name__)

# Retryable conditions
# QUALITY-02: 429 included — VOOI may rate-limit; retry with Retry-After header support
_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_BASE_BACKOFF_SEC = 1.0
_DEFAULT_RETRY_AFTER_SEC = 5.0  # fallback when Retry-After header absent on 429


def _classify_error(exc: Exception, status_code: Optional[int] = None) -> str:
    """Map exception / status code to error_kind taxonomy."""
    if status_code is not None:
        if 400 <= status_code < 500:
            return "http_4xx"
        if 500 <= status_code < 600:
            return "http_5xx"
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.ConnectError):
        return "connect_failed"
    if isinstance(exc, httpx.RemoteProtocolError):
        return "ssl"
    return "parse"


def _redact_auth(headers: dict[str, str]) -> dict[str, str]:
    """Return headers copy with Authorization value redacted."""
    redacted = dict(headers)
    if "authorization" in {k.lower() for k in redacted}:
        for key in list(redacted.keys()):
            if key.lower() == "authorization":
                redacted[key] = "REDACTED"
    return redacted


def _safe_json(text: str) -> Any:
    """Parse JSON safely, return raw text on failure."""
    try:
        return json.loads(text)
    except Exception:
        return text


class VooiClient:
    """Async VOOI API client with audit interceptor and retry logic."""

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "VooiClient":
        await self._ensure_client()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _ensure_client(self) -> None:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=settings.vooi_api_base_url,
                headers={
                    "Authorization": f"Bearer {settings.vooi_api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
            )

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Execute request with audit logging and retry logic."""
        await self._ensure_client()
        # НОВЫЙ-11: assert is stripped under `python -O`; use a real check.
        if self._client is None:
            raise RuntimeError("VooiClient: httpx client failed to initialize")

        correlation_id = str(uuid.uuid4())
        request_body_str: Optional[str] = None
        if json_body is not None:
            request_body_str = json.dumps(json_body)

        # Snapshot redacted request headers — Authorization is masked but the
        # rest (User-Agent, Content-Type, Accept) are useful when comparing
        # successful vs failed aster requests.
        request_headers_redacted = _redact_auth(dict(self._client.headers))
        # Full URL for the file log (path + base url + querystring)
        full_url = str(self._client.build_request(method, path, params=params).url)

        last_exc: Optional[Exception] = None

        for attempt in range(1, _MAX_RETRIES + 1):
            # Pre-flight aster log: written BEFORE the network call so we have
            # a record even on timeout / connection error.
            await log_aster_request_sent(
                correlation_id=correlation_id,
                method=method,
                url=full_url,
                request_headers=request_headers_redacted,
                request_body=request_body_str,
                params=params,
                attempt=attempt,
            )

            start_ms = time.monotonic()
            response: Optional[httpx.Response] = None

            try:
                response = await self._client.request(
                    method,
                    path,
                    json=json_body,
                    params=params,
                )
                duration_ms = int((time.monotonic() - start_ms) * 1000)
                status = response.status_code
                response_text = response.text[:4096]  # cap stored response size
                response_headers_dict = dict(response.headers)

                # Log every call
                await insert_api_call_log(
                    correlation_id=correlation_id,
                    method=method,
                    path=path,
                    request_body=request_body_str,
                    response_status=status,
                    response_body=response_text,
                    duration_ms=duration_ms,
                    request_headers=request_headers_redacted,
                    response_headers=response_headers_dict,
                    params=params,
                    attempt=attempt,
                )

                if status in _RETRY_STATUS_CODES and attempt < _MAX_RETRIES:
                    if status == 429:
                        # Respect Retry-After header from rate limiter
                        retry_after_str = response.headers.get("Retry-After", "")
                        try:
                            backoff = float(retry_after_str)
                        except (ValueError, TypeError):
                            backoff = _DEFAULT_RETRY_AFTER_SEC
                    else:
                        backoff = _BASE_BACKOFF_SEC * (2 ** (attempt - 1))
                    log.warning(
                        "vooi_api_retry",
                        correlation_id=correlation_id,
                        attempt=attempt,
                        status=status,
                        backoff=backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue

                if status >= 400:
                    await insert_error_log(
                        correlation_id=correlation_id,
                        method=method,
                        path=path,
                        request_body=request_body_str,
                        error_kind=_classify_error(Exception(), status),
                        error_detail=response_text[:512],
                        response_status=status,
                        response_body=response_text,
                        duration_ms=duration_ms,
                        request_headers=request_headers_redacted,
                        response_headers=response_headers_dict,
                        params=params,
                        attempt=attempt,
                    )
                    response.raise_for_status()

                # Some VOOI endpoints (e.g. POST /exchange/leverage, POST /exchange/margin-mode,
                # 204 No Content) reply 2xx with an empty body. Treat that as success and
                # return an empty dict instead of trying to JSON-parse "" (which raises
                # "Expecting value: line 1 column 1 (char 0)").
                if not response.content or not response.text.strip():
                    return {}

                # Parse JSON response
                try:
                    return response.json()
                except Exception as parse_exc:
                    duration_ms2 = int((time.monotonic() - start_ms) * 1000)
                    await insert_error_log(
                        correlation_id=correlation_id,
                        method=method,
                        path=path,
                        request_body=request_body_str,
                        error_kind="parse",
                        error_detail=str(parse_exc),
                        response_status=status,
                        response_body=response_text,
                        duration_ms=duration_ms2,
                        request_headers=request_headers_redacted,
                        response_headers=response_headers_dict,
                        params=params,
                        attempt=attempt,
                    )
                    raise

            except httpx.TimeoutException as exc:
                duration_ms = int((time.monotonic() - start_ms) * 1000)
                last_exc = exc
                await insert_error_log(
                    correlation_id=correlation_id,
                    method=method,
                    path=path,
                    request_body=request_body_str,
                    error_kind="timeout",
                    error_detail=str(exc),
                    response_status=None,
                    response_body=None,
                    duration_ms=duration_ms,
                    request_headers=request_headers_redacted,
                    response_headers=None,
                    params=params,
                    attempt=attempt,
                )
                if attempt < _MAX_RETRIES:
                    backoff = _BASE_BACKOFF_SEC * (2 ** (attempt - 1))
                    log.warning(
                        "vooi_api_timeout_retry",
                        correlation_id=correlation_id,
                        attempt=attempt,
                        backoff=backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                raise

            except httpx.ConnectError as exc:
                duration_ms = int((time.monotonic() - start_ms) * 1000)
                last_exc = exc
                await insert_error_log(
                    correlation_id=correlation_id,
                    method=method,
                    path=path,
                    request_body=request_body_str,
                    error_kind="connect_failed",
                    error_detail=str(exc),
                    response_status=None,
                    response_body=None,
                    duration_ms=duration_ms,
                    request_headers=request_headers_redacted,
                    response_headers=None,
                    params=params,
                    attempt=attempt,
                )
                raise

            except httpx.HTTPStatusError:
                raise

            except Exception as exc:
                duration_ms = int((time.monotonic() - start_ms) * 1000)
                last_exc = exc
                await insert_error_log(
                    correlation_id=correlation_id,
                    method=method,
                    path=path,
                    request_body=request_body_str,
                    error_kind=_classify_error(exc),
                    error_detail=str(exc),
                    response_status=None,
                    response_body=None,
                    duration_ms=duration_ms,
                    request_headers=request_headers_redacted,
                    response_headers=None,
                    params=params,
                    attempt=attempt,
                )
                raise

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Unexpected end of retry loop")

    async def get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        return await self._request("GET", path, params=params)

    async def post(self, path: str, body: Optional[dict[str, Any]] = None) -> Any:
        return await self._request("POST", path, json_body=body)

    async def delete(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
        json_body: Optional[dict[str, Any]] = None,
    ) -> Any:
        return await self._request("DELETE", path, params=params, json_body=json_body)

    async def get_current_price(self, symbol: str, exchange: str) -> float:
        """Fetch current market price from REST quotes endpoint."""
        data = await self.get(
            "/exchange/quotes",
            params={
                "asset": symbol,
                "exchanges": exchange,
                "side": "buy",
                "quoteSize": "1",
                "leverage": "1",
            },
        )
        if isinstance(data, list) and data:
            quote = data[0].get("quote") if isinstance(data[0], dict) else None
            if isinstance(quote, dict) and quote.get("averageExecutionPrice") is not None:
                return float(quote["averageExecutionPrice"])
        raise ValueError(f"Cannot extract price from quotes response: {data}")


# Module-level singleton (lazy-initialized)
_client_instance: Optional[VooiClient] = None


def get_vooi_client() -> VooiClient:
    """Get or create the module-level VooiClient singleton."""
    global _client_instance
    if _client_instance is None:
        _client_instance = VooiClient()
    return _client_instance

"""
LLM client wrapper.
Thin wrapper around OpenAI SDK with retry and logging.
Used by bot/parser.py.
"""
import json
from typing import Any, Optional

import structlog
from openai import AsyncOpenAI

from bot.config import settings

log = structlog.get_logger(__name__)

_client: Optional[AsyncOpenAI] = None


def get_llm_client() -> AsyncOpenAI:
    """Get or create the OpenAI client singleton."""
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=settings.llm_api_key)
    return _client


async def chat_complete(
    system_prompt: str,
    user_message: str,
    model: Optional[str] = None,
    temperature: float = 0.0,
    max_tokens: int = 1024,
    json_mode: bool = True,
) -> Optional[str]:
    """
    Execute a chat completion request.
    Returns the response content string or None on failure.
    """
    client = get_llm_client()
    used_model = model or settings.llm_model

    kwargs: dict[str, Any] = {
        "model": used_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    try:
        response = await client.chat.completions.create(**kwargs)
        content = response.choices[0].message.content
        log.debug(
            "llm_request_completed",
            model=used_model,
            prompt_tokens=response.usage.prompt_tokens if response.usage else None,
            completion_tokens=response.usage.completion_tokens if response.usage else None,
        )
        return content
    except Exception as e:
        log.error("llm_request_failed", model=used_model, error=str(e))
        return None

"""
Crash diagnostics installed at process start.

The bot has exhibited a recurring 'silent death' pattern: exit code 1
(occasionally 144) after 20m–17h of uptime, with no traceback, no
``Shutdown complete.`` line, no ``Task X failed`` message — last log
line is always a normal HTTP 200. This module wires up every safety
net Python offers so that the next crash leaves a usable trail.

Output goes to ``logs/crash.log`` (append, line-buffered), separate
from the harness stdout — if stdout is what gets killed first, the
diagnostic still lands on disk.

Hooks installed:

* ``faulthandler.enable`` — dumps Python stack of every thread on
  SIGSEGV / SIGFPE / SIGBUS / SIGILL / SIGABRT (catches C-level crashes
  in asyncpg / Telethon / httpx).
* ``faulthandler.register(SIGUSR1)`` — ``kill -USR1 <pid>`` produces a
  live all-threads stack dump without disturbing the process.
* ``sys.excepthook`` — uncaught exceptions in the main thread.
* ``threading.excepthook`` — uncaught exceptions in any thread.
* ``atexit.register`` — interpreter is exiting; logs uptime and any
  active exception at exit time.
* ``signal.signal(SIGTERM/SIGINT/SIGHUP/SIGPIPE)`` — logs the signal
  and current frame, then re-raises the default handler.
* ``loop.set_exception_handler`` — unhandled exceptions inside any
  asyncio task or callback (the most common silent-failure path).
"""
from __future__ import annotations

import atexit
import faulthandler
import os
import signal
import sys
import threading
import time
import traceback
from typing import IO, Optional

import structlog

log = structlog.get_logger(__name__)

_crash_file: Optional[IO[str]] = None
_started_at: float = 0.0


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _uptime_s() -> float:
    return time.time() - _started_at if _started_at else 0.0


def _write(*lines: str) -> None:
    """Best-effort append to crash.log + stderr. Cannot raise."""
    msg = "\n".join(line for line in lines if line) + "\n"
    if _crash_file is not None:
        try:
            _crash_file.write(msg)
            _crash_file.flush()
        except Exception:
            pass
    try:
        sys.stderr.write(msg)
        sys.stderr.flush()
    except Exception:
        pass


def _on_unhandled_exception(exc_type, exc_value, exc_tb) -> None:
    _write(
        f"[{_now()}] FATAL: unhandled exception in main thread, uptime={_uptime_s():.1f}s",
        "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
    )
    sys.__excepthook__(exc_type, exc_value, exc_tb)


def _on_threading_exception(args) -> None:
    name = getattr(args.thread, "name", "?") if args.thread else "?"
    _write(
        f"[{_now()}] FATAL: unhandled exception in thread '{name}', uptime={_uptime_s():.1f}s",
        "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)),
    )


def _on_atexit() -> None:
    et, ev, tb = sys.exc_info()
    extra = ""
    if et is not None:
        extra = "active exception at exit:\n" + "".join(
            traceback.format_exception(et, ev, tb)
        )
    _write(
        f"[{_now()}] atexit: interpreter exiting, uptime={_uptime_s():.1f}s",
        extra,
    )


def _on_signal(signum: int, frame) -> None:
    try:
        name = signal.Signals(signum).name
    except ValueError:
        name = str(signum)
    _write(
        f"[{_now()}] SIGNAL: received {name} ({signum}), uptime={_uptime_s():.1f}s",
        "frame stack:" if frame else "",
        "".join(traceback.format_stack(frame)) if frame else "",
    )
    # Restore default and re-raise so normal SIGTERM->graceful-shutdown path runs
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


def install_diagnostics(log_dir: str = "logs") -> None:
    """Wire every safety net for diagnosing silent crashes. Idempotent-safe."""
    global _crash_file, _started_at
    if _crash_file is not None:
        return  # already installed

    _started_at = time.time()
    os.makedirs(log_dir, exist_ok=True)
    crash_path = os.path.join(log_dir, "crash.log")
    # line-buffered append; survives parent-stdout closure
    _crash_file = open(crash_path, "a", buffering=1, encoding="utf-8")

    _write(
        "=" * 72,
        f"[{_now()}] crash_diagnostics installed",
        f"  pid={os.getpid()} python={sys.version.split()[0]} cwd={os.getcwd()}",
    )

    # 1. C-level safety net: dumps Python stacks on fatal signals
    faulthandler.enable(file=_crash_file)
    try:
        faulthandler.register(signal.SIGUSR1, file=_crash_file, all_threads=True)
    except (AttributeError, ValueError):
        pass  # platform may not support SIGUSR1

    # 2. Python-level safety nets
    sys.excepthook = _on_unhandled_exception
    threading.excepthook = _on_threading_exception
    atexit.register(_on_atexit)

    # 3. Log catchable signals (do NOT swallow — re-raise default)
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGPIPE):
        try:
            signal.signal(sig, _on_signal)
        except (OSError, ValueError):
            pass


def install_asyncio_handler(loop) -> None:
    """Install asyncio exception handler. Call from inside the running loop."""

    def _handler(loop, context):
        msg = context.get("message", "")
        exc = context.get("exception")
        task = context.get("task")
        future = context.get("future")
        protocol = context.get("protocol")

        task_name = task.get_name() if task is not None else "—"
        exc_tb = (
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            if exc is not None
            else ""
        )
        _write(
            f"[{_now()}] ASYNCIO unhandled: {msg}",
            f"  uptime={_uptime_s():.1f}s task={task_name} future={future!r} protocol={protocol!r}",
            exc_tb,
        )
        try:
            log.error(
                "asyncio_unhandled",
                message=msg,
                task=task_name,
                exception=repr(exc) if exc else None,
            )
        except Exception:
            pass

    loop.set_exception_handler(_handler)
    _write(f"[{_now()}] asyncio exception_handler installed")

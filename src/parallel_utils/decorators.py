from typing import Callable, Any, Union, List, Awaitable, TypeVar, Optional, Coroutine
import logging

# Project-specific imports
from .base import (
    create_async_parallel_decorator, 
    create_sync_parallel_decorator,
    _default_logger as _base_default_logger # Assuming you want to use the base logger config
)
from .engines.async_engine import execute_single_async, execute_list_async
from .engines.sync_engine import execute_single_sync, execute_list_sync

T = TypeVar('T')

# You can use the base logger or define a new one for this module if needed
_default_logger = _base_default_logger # or logging.getLogger("parallel_utils.decorators")

# --- Public Decorators --- 

def parallel(
    max_workers: int = 10,
    per_task_timeout: Optional[float] = None,
    return_exceptions: bool = False,
    logger: Optional[logging.Logger] = None
) -> Callable[
    [Callable[..., Awaitable[T]]], # Decorator accepts an async function
    Callable[[Any], Coroutine[Any, Any, Union[List[Union[T, Exception, None]], Union[T, Exception, None]]]]
]:
    """
    Decorator to run an async function.
    If called with a list as the first argument, it runs the function in parallel for each item.
    If called with a single item (or a tuple of args for the wrapped function), it runs once.

    This is designed for I/O-bound async functions. It uses asyncio.Semaphore
    to limit concurrency and asyncio.gather to run tasks for parallel execution.
    (Full docstring details can be copied from the old parallel.py or README)
    """
    return create_async_parallel_decorator(
        engine_execute_single_async=execute_single_async,
        engine_execute_list_async=execute_list_async,
        max_workers=max_workers,
        per_task_timeout=per_task_timeout,
        return_exceptions=return_exceptions,
        logger=(logger or _default_logger)
    )

def parallel_threaded(
    max_workers: int = 10,
    per_task_timeout: Optional[float] = None,
    return_exceptions: bool = False,
    logger: Optional[logging.Logger] = None
) -> Callable[
    [Callable[..., T]], # Decorator accepts a sync function
    Callable[[Any], Union[List[Union[T, Exception, None]], Union[T, Exception, None]]]
]:
    """
    Decorator to run a synchronous function.
    If called with a list as the first argument, it runs the function in parallel for each item
    using a ThreadPoolExecutor.
    If called with a single item (or a tuple of args for the wrapped function), it runs once.

    This is designed for CPU-bound functions or synchronous functions performing blocking I/O.
    (Full docstring details can be copied from the old parallel.py or README)
    """
    return create_sync_parallel_decorator(
        engine_execute_single_sync=execute_single_sync,
        engine_execute_list_sync=execute_list_sync,
        max_workers=max_workers,
        per_task_timeout=per_task_timeout,
        return_exceptions=return_exceptions,
        logger=(logger or _default_logger)
    )
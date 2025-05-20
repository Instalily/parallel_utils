from typing import Callable, Any, Union, List, Awaitable, TypeVar, Optional, Coroutine, Dict
import logging
from functools import wraps
import concurrent.futures
import asyncio

T = TypeVar('T')  # For return type of the decorated function

# Setup a default logger for the library
_default_logger = logging.getLogger(__name__)
if not _default_logger.hasHandlers(): # Avoid adding multiple handlers if already configured
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(_formatter)
    _default_logger.addHandler(_handler)
    _default_logger.setLevel(logging.WARNING) # Default to WARNING, user can change

# --- Internal Execution Strategies for LISTS --- 

async def _asyncio_execution_strategy(
    func: Callable[..., Awaitable[T]], # Expects the original async func
    processed_args_list: List[tuple],
    max_workers: int,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    actual_logger: logging.Logger
) -> List[Union[T, Exception, None]]:
    """Handles PARALLEL execution for a LIST of asyncio tasks."""
    semaphore = asyncio.Semaphore(max_workers)
    tasks: List[asyncio.Task] = []

    async def run_single_item_under_semaphore(item_args_tuple: tuple) -> Union[T, Exception, None]:
        async with semaphore:
            actual_logger.debug(f"Starting async task (under semaphore) with args: {item_args_tuple}")
            try:
                if per_task_timeout is not None:
                    return await asyncio.wait_for(func(*item_args_tuple), timeout=per_task_timeout)
                else:
                    return await func(*item_args_tuple)
            except asyncio.TimeoutError as e_timeout:
                actual_logger.warning(f"Async task timed out for args: {item_args_tuple}")
                return e_timeout if return_exceptions else None
            except Exception as e:
                actual_logger.error(f"Async task failed for args {item_args_tuple}: {e}", exc_info=True)
                return e if return_exceptions else None

    for item_tuple in processed_args_list:
        tasks.append(asyncio.create_task(run_single_item_under_semaphore(item_tuple)))
    
    return await asyncio.gather(*tasks, return_exceptions=False) # Individual exceptions handled by run_single_item_under_semaphore

def _threaded_execution_strategy(
    func: Callable[..., T], # Expects the original sync func
    processed_args_list: List[tuple],
    max_workers: int,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    actual_logger: logging.Logger
) -> List[Union[T, Exception, None]]:
    """Handles PARALLEL execution for a LIST of threaded tasks."""
    results: List[Union[T, Exception, None]] = [None] * len(processed_args_list)
    future_to_index: Dict[concurrent.futures.Future, int] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, item_tuple in enumerate(processed_args_list):
            actual_logger.debug(f"Submitting parallel task to thread pool with args: {item_tuple}")
            future = executor.submit(func, *item_tuple)
            future_to_index[future] = i
        
        for future in concurrent.futures.as_completed(future_to_index.keys()):
            original_index = future_to_index[future]
            item_args_for_log = processed_args_list[original_index]
            try:
                result_value = future.result(timeout=per_task_timeout)
                results[original_index] = result_value
            except concurrent.futures.TimeoutError as e_timeout:
                actual_logger.warning(f"Threaded task timed out for args: {item_args_for_log}")
                future.cancel()
                results[original_index] = e_timeout if return_exceptions else None
            except Exception as e:
                actual_logger.error(f"Threaded task failed for args {item_args_for_log}: {e}", exc_info=True)
                results[original_index] = e if return_exceptions else None
    return results

# --- Public Decorators --- 

def parallel(
    max_workers: int = 10,
    per_task_timeout: Optional[float] = None,
    return_exceptions: bool = False,
    logger: Optional[logging.Logger] = None
) -> Callable[ # This is the type of the decorator itself
    [Callable[..., Awaitable[T]]], # It takes an async function
    Callable[ # And returns a new function (the wrapper)
        [Any], # Which takes Any (input_arg)
        Coroutine[Any, Any, Union[List[Union[T, Exception, None]], Union[T, Exception, None]]] # And returns a Coroutine
    ]
]:
    """
    Decorator to run an async function.
    If called with a list as the first argument, it runs the function in parallel for each item.
    If called with a single item (or a tuple of args for the wrapped function), it runs once.

    This is designed for I/O-bound async functions. It uses asyncio.Semaphore
    to limit concurrency and asyncio.gather to run tasks for parallel execution.

    Args:
        max_workers: Maximum number of concurrent tasks for parallel execution.
        per_task_timeout: Timeout in seconds for each individual call to the wrapped function.
                          Applies to both single and parallel calls.
        return_exceptions: If True, exceptions from tasks (including timeouts) are returned.
                           If False (default), None is returned for tasks that raise an
                           exception or time out.
        logger: Optional custom logger. If None, a default module logger is used.
    """
    actual_logger = logger or _default_logger

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[[Any], Coroutine[Any, Any, Union[List[Union[T, Exception, None]], Union[T, Exception, None]]]]:
        @wraps(func)
        async def wrapper(input_arg: Any) -> Union[List[Union[T, Exception, None]], Union[T, Exception, None]]:
            if isinstance(input_arg, list):
                args_list = input_arg
                if not args_list: return []
                processed_args_list = [(arg,) if not isinstance(arg, tuple) else arg for arg in args_list]
                return await _asyncio_execution_strategy(func, processed_args_list, max_workers, per_task_timeout, return_exceptions, actual_logger)
            else:
                # Single async execution
                single_item_tuple_args = (input_arg,) if not isinstance(input_arg, tuple) else input_arg
                actual_logger.debug(f"Starting single async task with args: {single_item_tuple_args}")
                try:
                    if per_task_timeout is not None:
                        return await asyncio.wait_for(func(*single_item_tuple_args), timeout=per_task_timeout)
                    else:
                        return await func(*single_item_tuple_args)
                except asyncio.TimeoutError as e_timeout:
                    actual_logger.warning(f"Single async task timed out for args: {single_item_tuple_args}")
                    return e_timeout if return_exceptions else None
                except Exception as e:
                    actual_logger.error(f"Single async task failed for args {single_item_tuple_args}: {e}", exc_info=True)
                    return e if return_exceptions else None
        return wrapper
    return decorator

def parallel_threaded(
    max_workers: int = 10,
    per_task_timeout: Optional[float] = None,
    return_exceptions: bool = False,
    logger: Optional[logging.Logger] = None
) -> Callable[ # This is the type of the decorator itself
    [Callable[..., T]], # It takes a sync function
    Callable[ # And returns a new function (the wrapper)
        [Any], # Which takes Any (input_arg)
        Union[List[Union[T, Exception, None]], Union[T, Exception, None]] # And returns a direct result or list of results
    ]
]:
    """
    Decorator to run a synchronous function.
    If called with a list as the first argument, it runs the function in parallel for each item
    using a ThreadPoolExecutor.
    If called with a single item (or a tuple of args for the wrapped function), it runs once.

    This is designed for CPU-bound functions or synchronous functions performing blocking I/O.

    Args:
        max_workers: Maximum number of worker threads for parallel execution.
        per_task_timeout: Timeout in seconds for each individual call to the wrapped function.
                          Applies to both single and parallel calls when using threads.
        return_exceptions: If True, exceptions from tasks (including timeouts) are returned.
                           If False (default), None is returned for tasks that raise an
                           exception or time out.
        logger: Optional custom logger. If None, a default module logger is used.
    """
    actual_logger = logger or _default_logger

    def decorator(func: Callable[..., T]) -> Callable[[Any], Union[List[Union[T, Exception, None]], Union[T, Exception, None]]]:
        @wraps(func)
        def wrapper(input_arg: Any) -> Union[List[Union[T, Exception, None]], Union[T, Exception, None]]:
            if isinstance(input_arg, list):
                args_list = input_arg
                if not args_list: return []
                processed_args_list = [(arg,) if not isinstance(arg, tuple) else arg for arg in args_list]
                return _threaded_execution_strategy(func, processed_args_list, max_workers, per_task_timeout, return_exceptions, actual_logger)
            else:
                # Single threaded execution
                single_item_tuple_args = (input_arg,) if not isinstance(input_arg, tuple) else input_arg
                actual_logger.debug(f"Submitting single sync task with args: {single_item_tuple_args}")
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as temp_executor:
                    future = temp_executor.submit(func, *single_item_tuple_args)
                    try:
                        return future.result(timeout=per_task_timeout)
                    except concurrent.futures.TimeoutError as e_timeout:
                        actual_logger.warning(f"Single sync task timed out for args: {single_item_tuple_args}")
                        future.cancel()
                        return e_timeout if return_exceptions else None
                    except Exception as e:
                        actual_logger.error(f"Single sync task failed for args {single_item_tuple_args}: {e}", exc_info=True)
                        return e if return_exceptions else None
        return wrapper
    return decorator
    
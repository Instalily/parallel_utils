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

def parallel(
    max_workers: int = 10,
    per_task_timeout: Optional[float] = None,
    return_exceptions: bool = False,
    logger: Optional[logging.Logger] = None
):
    """
    Decorator to run an async function in parallel for a list of arguments.

    This is designed for I/O-bound async functions. It uses asyncio.Semaphore
    to limit concurrency and asyncio.gather to run tasks.

    Args:
        max_workers: Maximum number of concurrent tasks.
        per_task_timeout: Timeout in seconds for each individual call to the wrapped function.
                          If a task times out, its result will be None or the TimeoutError
                          if return_exceptions is True.
        return_exceptions: If True, exceptions from tasks (including timeouts) are returned
                           as part of the results list. If False (default), None is
                           returned for tasks that raise an exception or time out.
        logger: Optional custom logger. If None, a default module logger is used.
    """
    actual_logger = logger or _default_logger

    # The decorator takes the user's async function (func)
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[[List[Any]], Coroutine[Any, Any, List[Union[T, Exception, None]]]]:
        @wraps(func) # Preserves metadata of the original function
        async def wrapper(args_list: List[Any]) -> List[Union[T, Exception, None]]:
            if not args_list:
                return []

            # Ensure all arguments in args_list are tuples, so they can be *unpacked
            processed_args_list = [
                (arg,) if not isinstance(arg, tuple) else arg
                for arg in args_list
            ]

            semaphore = asyncio.Semaphore(max_workers)
            tasks: List[asyncio.Task] = []

            async def task_runner(item_args: tuple) -> Union[T, Exception, None]:
                # Acquire semaphore before starting the task
                async with semaphore:
                    actual_logger.debug(f"Starting task with args: {item_args}")
                    try:
                        # Apply per-task timeout if specified
                        if per_task_timeout is not None:
                            return await asyncio.wait_for(func(*item_args), timeout=per_task_timeout)
                        else:
                            return await func(*item_args)
                    except asyncio.TimeoutError as e_timeout:
                        actual_logger.warning(f"Task timed out for args: {item_args}")
                        return e_timeout if return_exceptions else None
                    except Exception as e:
                        actual_logger.error(f"Task failed for args {item_args}: {e}", exc_info=True)
                        return e if return_exceptions else None

            for item_args in processed_args_list:
                # Create an asyncio.Task for each operation
                tasks.append(asyncio.create_task(task_runner(item_args)))

            # asyncio.gather runs all tasks concurrently (respecting the semaphore indirectly
            # as each task_runner acquires it). It returns results in the order of tasks provided.
            # The return_exceptions=True here is for gather itself, but our task_runner
            # already handles exceptions and decides what to return based on the decorator's
            # return_exceptions flag.
            results = await asyncio.gather(*tasks, return_exceptions=False) # task_runner handles individual exceptions
            return results

        return wrapper
    return decorator

def parallel_threaded(
    max_workers: int = 10,
    per_task_timeout: Optional[float] = None, # Timeout for each individual task
    return_exceptions: bool = False,
    logger: Optional[logging.Logger] = None
):
    """
    Decorator to run a synchronous function in parallel for a list of arguments
    using a ThreadPoolExecutor.

    This is designed for CPU-bound functions or synchronous functions performing
    blocking I/O.

    Args:
        max_workers: Maximum number of worker threads.
        per_task_timeout: Timeout in seconds for each individual call to the wrapped function.
                          If a task times out, its result will be None or the TimeoutError
                          if return_exceptions is True.
        return_exceptions: If True, exceptions from tasks (including timeouts) are returned
                           as part of the results list. If False (default), None is
                           returned for tasks that raise an exception or time out.
        logger: Optional custom logger. If None, a default module logger is used.
    """
    actual_logger = logger or _default_logger

    # The decorator takes the user's synchronous function (func)
    def decorator(func: Callable[..., T]) -> Callable[[List[Any]], List[Union[T, Exception, None]]]:
        @wraps(func) # Preserves metadata of the original function
        def wrapper(args_list: List[Any]) -> List[Union[T, Exception, None]]:
            if not args_list:
                return []

            processed_args_list = [
                (arg,) if not isinstance(arg, tuple) else arg
                for arg in args_list
            ]

            results: List[Union[T, Exception, None]] = [None] * len(processed_args_list)
            # We need to map futures back to their original index if order is to be preserved.
            # A dictionary can help: future -> original_index
            future_to_index: Dict[concurrent.futures.Future, int] = {}

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for i, item_args in enumerate(processed_args_list):
                    actual_logger.debug(f"Submitting task to thread pool with args: {item_args}")
                    future = executor.submit(func, *item_args)
                    future_to_index[future] = i

                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_index.keys()):
                    original_index = future_to_index[future]
                    item_args_for_log = processed_args_list[original_index]
                    try:
                        # future.result() will block until the future is done,
                        # or per_task_timeout is reached if specified.
                        result_value = future.result(timeout=per_task_timeout)
                        results[original_index] = result_value
                    except concurrent.futures.TimeoutError as e_timeout:
                        actual_logger.warning(f"Threaded task timed out for args: {item_args_for_log}")
                        results[original_index] = e_timeout if return_exceptions else None
                    except Exception as e:
                        actual_logger.error(f"Threaded task failed for args {item_args_for_log}: {e}", exc_info=True)
                        results[original_index] = e if return_exceptions else None
            
            return results
        return wrapper
    return decorator
    
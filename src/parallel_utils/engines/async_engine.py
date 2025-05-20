# Async execution engine logic
from typing import Callable, Any, Union, List, Awaitable, TypeVar, Optional
import asyncio
import logging

T = TypeVar('T')
# Logger for this specific engine module
_logger = logging.getLogger("parallel_utils.engines.async") 
if not _logger.hasHandlers():
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(_formatter)
    _logger.addHandler(_handler)
    _logger.setLevel(logging.WARNING)

# Functions for this engine will be defined here
# e.g., execute_single_async, execute_list_async 

async def execute_single_async(
    func: Callable[..., Awaitable[T]],
    item_args_tuple: tuple,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    # logger is passed in, not using module _logger directly in core logic
    # to allow user's logger to be used if provided by base decorator
    logger: logging.Logger 
) -> Union[T, Exception, None]:
    logger.debug(f"Executing single async task with args: {item_args_tuple}")
    try:
        if per_task_timeout is not None:
            return await asyncio.wait_for(func(*item_args_tuple), timeout=per_task_timeout)
        else:
            return await func(*item_args_tuple)
    except asyncio.TimeoutError as e_timeout:
        logger.warning(f"Async task timed out for args: {item_args_tuple}")
        return e_timeout if return_exceptions else None
    except Exception as e:
        logger.error(f"Async task failed for args {item_args_tuple}: {e}", exc_info=True)
        return e if return_exceptions else None

async def execute_list_async(
    func: Callable[..., Awaitable[T]],
    processed_args_list: List[tuple],
    max_workers: int,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    logger: logging.Logger
) -> List[Union[T, Exception, None]]:
    """Handles PARALLEL execution for a LIST of asyncio tasks."""
    semaphore = asyncio.Semaphore(max_workers)
    tasks: List[asyncio.Task] = []

    async def run_item_under_semaphore(item_args_tuple: tuple) -> Union[T, Exception, None]:
        async with semaphore:
            # Reuse the single item execution logic for consistency
            return await execute_single_async(
                func, item_args_tuple, per_task_timeout, return_exceptions, logger
            )

    if not processed_args_list: # Handle empty list explicitly
        return []

    for item_tuple in processed_args_list:
        tasks.append(asyncio.create_task(run_item_under_semaphore(item_tuple)))
    
    # The exceptions are already handled by execute_single_async, so gather doesn't need to return_exceptions=True
    return await asyncio.gather(*tasks, return_exceptions=False) 
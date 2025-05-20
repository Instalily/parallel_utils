# Sync execution engine logic
from typing import Callable, Any, Union, List, TypeVar, Optional, Dict
import concurrent.futures
import logging

T = TypeVar('T')
# Logger for this specific engine module
_logger = logging.getLogger("parallel_utils.engines.sync") 
if not _logger.hasHandlers():
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(_formatter)
    _logger.addHandler(_handler)
    _logger.setLevel(logging.WARNING)

# Functions for this engine will be defined here
# e.g., execute_single_sync, execute_list_sync 

def execute_single_sync(
    func: Callable[..., T],
    item_args_tuple: tuple,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    logger: logging.Logger
) -> Union[T, Exception, None]:
    logger.debug(f"Submitting single sync task with args: {item_args_tuple}")
    # For a single, timed call, always use a temporary executor
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as temp_executor:
        future = temp_executor.submit(func, *item_args_tuple)
        try:
            return future.result(timeout=per_task_timeout)
        except concurrent.futures.TimeoutError as e_timeout:
            logger.warning(f"Single sync task timed out for args: {item_args_tuple}")
            future.cancel() # Attempt to cancel the future
            return e_timeout if return_exceptions else None
        except Exception as e:
            logger.error(f"Single sync task failed for args {item_args_tuple}: {e}", exc_info=True)
            return e if return_exceptions else None

def execute_list_sync(
    func: Callable[..., T],
    processed_args_list: List[tuple],
    max_workers: int,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    logger: logging.Logger
) -> List[Union[T, Exception, None]]:
    """Handles PARALLEL execution for a LIST of threaded tasks."""
    if not processed_args_list: # Handle empty list
        return []

    results: List[Union[T, Exception, None]] = [None] * len(processed_args_list)
    future_to_index: Dict[concurrent.futures.Future, int] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, item_tuple in enumerate(processed_args_list):
            logger.debug(f"Submitting parallel task to thread pool with args: {item_tuple}")
            future = executor.submit(func, *item_tuple)
            future_to_index[future] = i
        
        for future in concurrent.futures.as_completed(future_to_index.keys()):
            original_index = future_to_index[future]
            item_args_for_log = processed_args_list[original_index]
            try:
                result_value = future.result(timeout=per_task_timeout)
                results[original_index] = result_value
            except concurrent.futures.TimeoutError as e_timeout:
                logger.warning(f"Threaded task timed out for args: {item_args_for_log}")
                future.cancel()
                results[original_index] = e_timeout if return_exceptions else None
            except Exception as e:
                logger.error(f"Threaded task failed for args {item_args_for_log}: {e}", exc_info=True)
                results[original_index] = e if return_exceptions else None
    return results 
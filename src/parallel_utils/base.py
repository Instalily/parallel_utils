# Base decorator logic will go here
from typing import Callable, Any, Union, List, Awaitable, TypeVar, Optional, Coroutine
import logging
from functools import wraps

T = TypeVar('T')
_default_logger = logging.getLogger("parallel_utils.base") # separate logger for base
if not _default_logger.hasHandlers():
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(_formatter)
    _default_logger.addHandler(_handler)
    _default_logger.setLevel(logging.WARNING)

# Define types for the wrappers clearly
AsyncWrapperType = Callable[[Any], Coroutine[Any, Any, Union[List[Union[T, Exception, None]], Union[T, Exception, None]]]]
SyncWrapperType = Callable[[Any], Union[List[Union[T, Exception, None]], Union[T, Exception, None]]]

# This will be the core factory
# It will expect 'engine_execute_single' and 'engine_execute_list' functions
# that match async or sync nature based on 'is_async_engine' flag. 

def create_async_parallel_decorator(
    engine_execute_single_async: Callable[..., Awaitable[Union[T, Exception, None]]],
    engine_execute_list_async: Callable[..., Awaitable[List[Union[T, Exception, None]]]],
    # Decorator params
    max_workers: int,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    logger: Optional[logging.Logger]
) -> Callable[[Callable[..., Awaitable[T]]], AsyncWrapperType[T]]: # Returns the decorator
    
    actual_logger = logger or _default_logger

    def decorator(func: Callable[..., Awaitable[T]]) -> AsyncWrapperType[T]:
        @wraps(func)
        async def async_wrapper(input_arg: Any) -> Union[List[Union[T, Exception, None]], Union[T, Exception, None]]:
            if isinstance(input_arg, list):
                args_list = input_arg
                if not args_list: return []
                processed_args_list = [(arg,) if not isinstance(arg, tuple) else arg for arg in args_list]
                return await engine_execute_list_async(
                    func, processed_args_list, max_workers, 
                    per_task_timeout, return_exceptions, actual_logger
                )
            else:
                single_item_tuple_args = (input_arg,) if not isinstance(input_arg, tuple) else input_arg
                return await engine_execute_single_async(
                    func, single_item_tuple_args, 
                    per_task_timeout, return_exceptions, actual_logger
                )
        return async_wrapper
    return decorator

def create_sync_parallel_decorator(
    engine_execute_single_sync: Callable[..., Union[T, Exception, None]],
    engine_execute_list_sync: Callable[..., List[Union[T, Exception, None]]],
    # Decorator params
    max_workers: int,
    per_task_timeout: Optional[float],
    return_exceptions: bool,
    logger: Optional[logging.Logger]
) -> Callable[[Callable[..., T]], SyncWrapperType[T]]: # Returns the decorator
    
    actual_logger = logger or _default_logger

    def decorator(func: Callable[..., T]) -> SyncWrapperType[T]:
        @wraps(func)
        def sync_wrapper(input_arg: Any) -> Union[List[Union[T, Exception, None]], Union[T, Exception, None]]:
            if isinstance(input_arg, list):
                args_list = input_arg
                if not args_list: return []
                processed_args_list = [(arg,) if not isinstance(arg, tuple) else arg for arg in args_list]
                return engine_execute_list_sync(
                    func, processed_args_list, max_workers, 
                    per_task_timeout, return_exceptions, actual_logger
                )
            else:
                single_item_tuple_args = (input_arg,) if not isinstance(input_arg, tuple) else input_arg
                return engine_execute_single_sync(
                    func, single_item_tuple_args, 
                    per_task_timeout, return_exceptions, actual_logger
                )
        return sync_wrapper
    return decorator 
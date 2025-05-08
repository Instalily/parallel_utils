from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Callable, Any, Union, List

def parallel(max_workers: int = 10) -> Callable:
    """
    Decorator to execute a function in parallel. The function can be called with either:
    1. A single argument: @parallel() def f(x): ...; f([1,2,3])
    2. Multiple arguments: @parallel() def f(x,y): ...; f([(1,2), (3,4), (5,6)])

    Args:
        batch_size: The number of arguments to process in each batch.
        max_workers: The maximum number of threads to use.
    """
    def _parallel(func: Callable):
        def wrapper_execute_parallel(args_list: Union[List[Any], List[tuple]]) -> List[Any]:
            # If args_list is a list of non-tuples, convert to list of single-element tuples
            if args_list and not isinstance(args_list[0], tuple):
                args_list = [(arg,) for arg in args_list]
            return execute(func, args_list, max_workers)
        return wrapper_execute_parallel
    return _parallel

def execute(func: Callable, args: List[tuple], max_workers: int = 10) -> List[Any]:
    """
    Execute a function in parallel for a list of arguments. You may want to consider also wrapping
    your function in a module like backoff to handle rate limiting or other errors. If there is an
    error, the function will return None for that argument.

    Args:
        func: The function to execute.
        args: The list of arguments to pass to the function.
        batch_size: The number of arguments to pass to the function in each batch.
        max_workers: The maximum number of threads to use.

    Returns:
        A list of results from the function.
    """
    n = len(args)
    futures: List[Future] = []

    def thread_func(thread_id: int):
        thread_index = thread_id
        local_results = []
        while thread_index < n:
            try:
                result = func(*args[thread_index])
                local_results.append(result)
            except Exception as e:
                print(f"Error processing args[{thread_index}]: {e}")
                local_results.append(None)
            thread_index += max_workers
        return local_results

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        for i in range(max_workers):
            futures.append(executor.submit(thread_func, i))

        # Collect results as they complete
        results = []
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                print(f"Thread error: {e}")

    return results
    
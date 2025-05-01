from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

def parallel(func: Callable, batch_size: int = 100, max_workers: int = 10) -> Callable:
    """
    Decorator to execute a function in parallel. The function should take a list of
    arguments and return a list of results. We make no gaurantees about the order of the results,
    or that all results will be returned. If you want to only execute a single function, you must
    pass a list with a single element.

    Args:
        func: The function to execute.
        batch_size: The number of arguments to pass to the function in each batch.
        max_workers: The maximum number of threads to use.
    """
    def wrapper_execute_parallel(args_list: list[tuple]):
        return execute(func, args_list, batch_size, max_workers)
    
    return wrapper_execute_parallel

def execute(func: Callable, args: list[tuple], batch_size: int = 100, max_workers: int = 10) -> list[tuple]:
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

    results = []
    for i in range(0, n, batch_size):
        current_batch_size = min(batch_size, n - i)
        current_batch = args[i:i+current_batch_size]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(func, *arg) for arg in current_batch]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Error in {current_batch}: {e}")
                    results.append(None)

    return results
    
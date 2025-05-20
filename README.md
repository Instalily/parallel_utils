# Parallel Utils

A Python library for parallel execution of functions, simplifying concurrent and parallel operations for both asynchronous I/O-bound tasks and synchronous CPU-bound/blocking I/O tasks.

Decorated functions can be called in two ways:
1.  With a list of arguments (or list of tuples of arguments) to execute in parallel.
2.  With a single argument (or a single tuple of arguments) for a direct, non-parallel call to the original function (still respecting timeouts).

## Installation

```bash
pip install parallel-utils
```

## Usage

`parallel-utils` provides two main decorators:

1.  `@parallel`: For `async def` functions (I/O-bound).
2.  `@parallel_threaded`: For regular `def` functions (CPU-bound or blocking I/O).

### 1. `@parallel` for Asynchronous I/O-bound Functions

Use `@parallel` when you have `async def` functions, typically performing network requests or other non-blocking I/O operations. It leverages `asyncio` for efficient concurrency when a list is provided.

```python
import asyncio
import aiohttp # Example: requires `pip install aiohttp`
from parallel_utils import parallel
import logging

# Configure basic logging to see output from the library's default logger
logging.basicConfig(level=logging.INFO)
_default_logger = logging.getLogger("parallel_utils") # Get the library's logger
_default_logger.setLevel(logging.INFO)


@parallel(max_workers=5, per_task_timeout=10.0, return_exceptions=True)
async def fetch_url_content(url: str, method: str = "GET") -> str:
    async with aiohttp.ClientSession() as session:
        _default_logger.info(f"Fetching {url} using {method}...")
        async with session.request(method, url) as response:
            response.raise_for_status() # Raise an exception for bad status codes
            return await response.text()

async def main_async():
    # --- Parallel Execution --- 
    urls_to_fetch_in_parallel = [
        ("http://example.com",),                # Single arg for fetch_url_content
        ("http://example.org", "POST"),        # Two args for fetch_url_content
        "http://example.net"                    # Can also be a list of single non-tuple args
    ]
    _default_logger.info("--- Running @parallel with a list (parallel execution) ---")
    parallel_results = await fetch_url_content(urls_to_fetch_in_parallel)

    for i, result in enumerate(parallel_results):
        original_arg_input = urls_to_fetch_in_parallel[i]
        if isinstance(result, Exception):
            _default_logger.error(f"Parallel task for {original_arg_input} failed: {type(result).__name__} - {result}")
        else:
            _default_logger.info(f"Parallel task for {original_arg_input} succeeded. Snippet: {str(result)[:50].replace('\n','\\n')}...")

    # --- Single Execution --- 
    _default_logger.info("\n--- Running @parallel with a single argument (direct call) ---")
    single_result = await fetch_url_content("http://httpbin.org/get") # Single non-tuple argument
    if isinstance(single_result, Exception):
        _default_logger.error(f"Single task failed: {type(single_result).__name__} - {single_result}")
    else:
        _default_logger.info(f"Single task succeeded. Snippet: {str(single_result)[:50].replace('\n','\\n')}...")

    _default_logger.info("\n--- Running @parallel with a single tuple of arguments (direct call) ---")
    single_result_post = await fetch_url_content(("http://httpbin.org/post", "POST")) # Single tuple argument
    if isinstance(single_result_post, Exception):
        _default_logger.error(f"Single POST task failed: {type(single_result_post).__name__} - {single_result_post}")
    else:
        _default_logger.info(f"Single POST task succeeded. Snippet: {str(single_result_post)[:50].replace('\n','\\n')}...")

# To run:
# if __name__ == "__main__":
# asyncio.run(main_async())
```

### 2. `@parallel_threaded` for Synchronous Functions (CPU-bound or Blocking I/O)

Use `@parallel_threaded` when you have regular `def` functions. It uses a `concurrent.futures.ThreadPoolExecutor` for parallel execution when a list is provided.

```python
import time
import os
import threading # For logging thread ID
from parallel_utils import parallel_threaded
import logging

# Configure basic logging (if not already done)
logging.basicConfig(level=logging.INFO)
_default_logger = logging.getLogger("parallel_utils")
_default_logger.setLevel(logging.INFO)


@parallel_threaded(max_workers=3, per_task_timeout=2.0, return_exceptions=True)
def simulate_blocking_task(task_id: int, duration: float) -> str:
    thread_id = threading.get_ident()
    _default_logger.info(f"PID: {os.getpid()}, Thread: {thread_id}: Starting blocking task {task_id} for {duration}s...")
    time.sleep(duration)
    if task_id == 1: # Simulate a task that fails
        raise ValueError(f"Simulated error in task {task_id}")
    _default_logger.info(f"PID: {os.getpid()}, Thread: {thread_id}: Finished blocking task {task_id}.")
    return f"Task {task_id} completed by thread {thread_id}"

def main_sync():
    # --- Parallel Execution --- 
    tasks_to_run_in_parallel = [
        (0, 1.5),   # (task_id, duration)
        1,          # Will be treated as (1,) for a function that expects one arg.
                    # Here, simulate_blocking_task expects two, so this would error
                    # unless the function handles default for the second arg, or we wrap it.
                    # For clarity, it's best to match the function signature.
        (1, 0.5),   # This task will raise an error
        (2, 2.5),   # This task will time out (per_task_timeout is 2.0s)
        (3, 0.8)
    ]
    # Let's make a more robust list for the example function:
    tasks_to_run_in_parallel_corrected = [
        (0, 1.5),
        (1, 0.5), 
        (2, 2.5),
        (3, 0.8)
    ]

    _default_logger.info("--- Running @parallel_threaded with a list (parallel execution) ---")
    parallel_results = simulate_blocking_task(tasks_to_run_in_parallel_corrected)

    for i, result in enumerate(parallel_results):
        original_args = tasks_to_run_in_parallel_corrected[i]
        if isinstance(result, Exception):
            _default_logger.error(f"Parallel task for {original_args} failed: {type(result).__name__} - {result}")
        else:
            _default_logger.info(f"Parallel task for {original_args} succeeded: {result}")

    # --- Single Execution --- 
    _default_logger.info("\n--- Running @parallel_threaded with single arguments (direct call) ---")
    single_result = simulate_blocking_task(4, 0.2) # Multiple arguments directly
    if isinstance(single_result, Exception):
        _default_logger.error(f"Single task (4, 0.2) failed: {type(single_result).__name__} - {single_result}")
    else:
        _default_logger.info(f"Single task (4, 0.2) succeeded: {single_result}")

    _default_logger.info("\n--- Running @parallel_threaded with a single tuple argument (direct call) ---")
    single_result_tuple = simulate_blocking_task((5, 0.3)) # Single tuple argument
    if isinstance(single_result_tuple, Exception):
        _default_logger.error(f"Single task (5, 0.3) failed: {type(single_result_tuple).__name__} - {single_result_tuple}")
    else:
        _default_logger.info(f"Single task (5, 0.3) succeeded: {single_result_tuple}")

# To run:
# if __name__ == "__main__":
# main_sync()
```

## Features

-   **Dual Decorators**:
    -   `@parallel` for efficient concurrency of `async def` I/O-bound tasks using `asyncio`.
    -   `@parallel_threaded` for true parallelism of synchronous CPU-bound tasks or for running blocking I/O tasks without freezing the main thread, using a `ThreadPoolExecutor`.
-   **Flexible Invocation**: Call decorated functions with a list of arguments for parallel execution or with a single set of arguments for a direct call.
-   **Flexible Argument Passing for Parallel Lists**: For parallel execution, the input list can contain individual items (for single-argument functions) or tuples of arguments (for multi-argument functions).
-   **Configurable Concurrency**: Control `max_workers` for parallel execution.
-   **Per-Task Timeouts**: Set `per_task_timeout` for individual operations, applicable to both single and parallel calls.
-   **Flexible Error Handling**: Choose to `return_exceptions` or get `None` for failed/timed-out tasks.
-   **Logging Integration**: Built-in logging for task status, errors, and timeouts. Customizable with your own logger.
-   **Type Hints**: Fully type-hinted for better IDE support and code quality.
-   **Order Preservation**: Results are returned in the same order as the input arguments for parallel calls.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Evan Vera - evan@instalily.ai

## Repository

This project is hosted on GitHub at [Instalily/parallel_utils](https://github.com/Instalily/parallel_utils). 
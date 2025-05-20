# Parallel Utils

A Python library for parallel execution of functions, simplifying concurrent and parallel operations for both asynchronous I/O-bound tasks and synchronous CPU-bound/blocking I/O tasks.

## Installation

```bash
pip install parallel-utils
```

## Usage

`parallel-utils` provides two main decorators:

1.  `@parallel`: For `async def` functions (I/O-bound).
2.  `@parallel_threaded`: For regular `def` functions (CPU-bound or blocking I/O).

### 1. `@parallel` for Asynchronous I/O-bound Functions

Use `@parallel` when you have `async def` functions, typically performing network requests or other non-blocking I/O operations. It leverages `asyncio` for efficient concurrency.

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
async def fetch_url_content(url: str) -> str:
    # For real usage, you'd likely want to reuse an aiohttp.ClientSession
    # See more advanced examples for how to pass shared resources.
    async with aiohttp.ClientSession() as session:
        _default_logger.info(f"Fetching {url}...")
        async with session.get(url) as response:
            response.raise_for_status() # Raise an exception for bad status codes
            return await response.text()

async def main_async():
    urls = [
        "http://example.com",
        "http://example.org",
        "http://nonexistentdomain-asdfjkl.com" # This will likely cause an error
    ]
    
    # The decorated function now accepts a list of arguments.
    # Each item in the list should be a tuple of arguments for the original function.
    # Since fetch_url_content takes one arg 'url', args_list is [(url1,), (url2,), ...]
    args_for_tasks = [(url,) for url in urls]
    results = await fetch_url_content(args_for_tasks)

    for i, result in enumerate(results):
        original_args = args_for_tasks[i]
        if isinstance(result, Exception):
            _default_logger.error(f"Task for {original_args} failed: {type(result).__name__} - {result}")
        else:
            _default_logger.info(f"Task for {original_args} succeeded. Result snippet: {str(result)[:50].replace('\n','\\n')}...")

# To run:
# if __name__ == "__main__":
# asyncio.run(main_async())
```

### 2. `@parallel_threaded` for Synchronous Functions (CPU-bound or Blocking I/O)

Use `@parallel_threaded` when you have regular `def` functions that are either:
*   CPU-intensive (e.g., complex calculations).
*   Perform blocking I/O (e.g., using the standard `requests` library, file operations).

It uses a `concurrent.futures.ThreadPoolExecutor` to run these functions in separate OS threads.

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
    # Each item in the list is a tuple of arguments for simulate_blocking_task
    tasks_to_run = [
        (0, 1.5),   # (task_id, duration)
        (1, 0.5),   # This task will raise an error
        (2, 2.5),   # This task will time out (per_task_timeout is 2.0s)
        (3, 0.8)
    ]
    
    results = simulate_blocking_task(tasks_to_run)

    for i, result in enumerate(results):
        original_args = tasks_to_run[i]
        if isinstance(result, Exception):
            _default_logger.error(f"Task for {original_args} failed: {type(result).__name__} - {result}")
        else:
            _default_logger.info(f"Task for {original_args} succeeded: {result}")

# To run:
# if __name__ == "__main__":
# main_sync()
```

## Features

-   **Dual Decorators**:
    -   `@parallel` for efficient concurrency of `async def` I/O-bound tasks using `asyncio`.
    -   `@parallel_threaded` for true parallelism of synchronous CPU-bound tasks or for running blocking I/O tasks without freezing the main thread, using a `ThreadPoolExecutor`.
-   **Configurable Concurrency**: Control `max_workers` for both decorators.
-   **Per-Task Timeouts**: Set `per_task_timeout` for individual operations.
-   **Flexible Error Handling**: Choose to `return_exceptions` or get `None` for failed/timed-out tasks.
-   **Logging Integration**: Built-in logging for task status, errors, and timeouts. Customizable with your own logger.
-   **Type Hints**: Fully type-hinted for better IDE support and code quality.
-   **Order Preservation**: `@parallel_threaded` preserves the order of results corresponding to the input arguments. `@parallel` (using `asyncio.gather`) also preserves order.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Evan Vera - evan@instalily.ai

## Repository

This project is hosted on GitHub at [Instalily/parallel_utils](https://github.com/Instalily/parallel_utils). 
# Parallel Utils

A Python library for parallel execution of functions, designed to simplify concurrent operations.

## Installation

```bash
pip install parallel-utils
```

## Usage

```python
from parallel_utils import parallel

# Decorate your function
@parallel
def my_function(arg):
    # Your function logic here
    return result

# Call the function with a list of arguments to execute in parallel
results = my_function([arg1, arg2, arg3])
```

You can also configure the parallel execution:

```python
@parallel(batch_size=2, max_workers=2)
def my_function(arg):
    # Your function logic here
    return result
```

## Features

- Simple decorator-based interface for parallel execution
- Configurable number of workers and batch size
- Built on Python's concurrent.futures
- Type hints for better IDE support

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Evan Vera - evan@instalily.ai

## Repository

This project is hosted on GitHub at [Instalily/parallel_utils](https://github.com/Instalily/parallel_utils). 
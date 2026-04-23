import time
from functools import wraps
from typing import Callable


def timeit(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        func_name = getattr(func, "__name__", repr(func))
        print(f"Function '{func_name}' executed in {elapsed_time:.4f} seconds")
        return result

    return wrapper

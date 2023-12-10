import time
from functools import wraps


def numerize(n) -> str:
    """display positive number n for readability

    Examples:
        >>> numerize(1_000)
        '1K'
        >>> numerize(1_000_000_000)
        '1B'
    """
    sufix2upbound = {
        "EMPTY": 1e3,
        "K": 1e6,
        "M": 1e9,
        "B": 1e12,
        "END": float('inf'),
    }

    display_n, sufix = n, ""
    for s, base in sufix2upbound.items():
        # number >= 1000B will alway have sufix 'B'
        if s == "END":
            display_n = int(n/1e9)
            sufix = "B"
            break

        if n < base:
            sufix = "" if s == "EMPTY" else s
            display_n = int(n/(base/1e3))
            break
    return f"{display_n}{sufix}"


def time_it(func):
    """
    计算函数运行时间
    
    Args:
        func (function): 需要被测量的函数，需要有@wraps装饰器。
    
    Returns:
        function: 返回一个带有运行时间戳和运行时间差值的函数。
    
    """
    @wraps(func)
    def inner(*args, **kwargs):
        pref = time.perf_counter()
        result = func(*args, **kwargs)
        delta = time.perf_counter() - pref
        return result, delta
    return inner

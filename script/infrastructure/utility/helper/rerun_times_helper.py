def rerun_times(times, func, *args):
    """重试方法，防止操作redis时由于偶然原因导致操作redis失败，加上了重试机制
    """
    if times <= 0:
        return
    tmp = times
    while tmp > 0:
        try:
            res = func(*args)
            return res
        except:
            tmp -= 1

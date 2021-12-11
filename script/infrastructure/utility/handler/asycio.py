import asyncio
import functools
import time

from concurrent.futures import ThreadPoolExecutor



class ThreadPool():

    def __init__(self, max_workers):

        self._thread_pool = ThreadPoolExecutor(max_workers)

    async def run(self, _callable, *args, **kwargs):

        future = self._thread_pool.submit(_callable, *args, **kwargs)

        return await asyncio.wrap_future(future)


class ThreadWorker:

    def __init__(self, max_workers):

        self._thread_pool = ThreadPool(max_workers)

    def __call__(self, func):

        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            return self._thread_pool.run(func, *args, **kwargs)

        return _wrapper


thread_worker = ThreadWorker(32)


@thread_worker
def some_io_block():
    print(1)
    asyncio.sleep(5)
    print(0)

@thread_worker
def some_io_block2():
    print(3)
    asyncio.sleep(5)
    print(4)


asyncio.run(some_io_block())
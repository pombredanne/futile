import asyncio
from functools import wraps, partial


def aio_wrap(loop=None, executor=None):
    outer_loop = loop
    outer_executor = executor

    def wrap(fn):
        @wraps(fn)
        async def run(*args, loop=None, executor=None, **kwargs):
            if loop is None:
                if outer_loop is None:
                    loop = asyncio.get_event_loop()
                else:
                    loop = outer_loop
            if executor is None:
                executor = outer_executor
            pfunc = partial(fn, *args, **kwargs)
            return await loop.run_in_executor(executor, pfunc)

        return run

    return wrap

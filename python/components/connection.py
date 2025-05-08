import aiohttp
import asyncio
import time

from typing import Optional


class ThrottledClientSession(aiohttp.ClientSession):
    """
    A rate-throttled client session class inherited from aiohttp.ClientSession.

    This class implements a rate-limiting mechanism using a leaky bucket algorithm
    to control the rate of requests made through the session.

    See https://stackoverflow.com/a/60357775/107049 for more details

    Attributes:
        MIN_SLEEP (float): The minimum sleep time between requests.
        rate_limit (float): The maximum number of requests allowed per second.
        _fillerTask (asyncio.Task): The task responsible for filling the rate-limiting bucket.
        _queue (asyncio.Queue): The queue used for rate-limiting.
        _start_time (float): The start time of the session.

    Args:
        rate_limit (float, optional): The maximum number of requests allowed per second.
            If None, no rate limiting is applied. Defaults to None.
        *args: Variable length argument list to be passed to aiohttp.ClientSession.
        **kwargs: Arbitrary keyword arguments to be passed to aiohttp.ClientSession.

    Raises:
        ValueError: If rate_limit is not positive when provided.

    Example:
        Replace `session = aiohttp.ClientSession()`
        with `session = ThrottledClientSession(rate_limit=15)`
    """

    MIN_SLEEP = 0.1

    def __init__(self, rate_limit: float = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rate_limit = rate_limit
        self._fillerTask = None
        self._queue = None
        self._start_time = time.time()
        if rate_limit is not None:
            if rate_limit <= 0:
                raise ValueError('rate_limit must be positive')
            self._queue = asyncio.Queue(min(2, int(rate_limit) + 1))
            self._fillerTask = asyncio.create_task(self._filler(rate_limit))

    def _get_sleep(self) -> Optional[float]:
        """Calculate the sleep time between requests based on the rate limit.

        Returns:
            Optional[float]: The sleep time in seconds, or None if no rate limit is set.
        """
        if self.rate_limit is not None:
            return max(1 / self.rate_limit, self.MIN_SLEEP)
        return None

    async def close(self) -> None:
        """Close the rate-limiter's "bucket filler" task and the underlying session.

        This method should be called to properly clean up the session and its resources.
        """
        if self._fillerTask is not None:
            self._fillerTask.cancel()
        try:
            await asyncio.wait_for(self._fillerTask, timeout=0.5)
        except asyncio.TimeoutError as err:
            print(str(err))
        await super().close()

    async def _filler(self, rate_limit: float = 1):
        """Filler task to implement the leaky bucket algorithm for rate limiting.

        Args:
            rate_limit (float, optional): The rate limit to use. Defaults to 1.
        """
        try:
            if self._queue is None:
                return
            self.rate_limit = rate_limit
            sleep = self._get_sleep()
            updated_at = time.monotonic()
            fraction = 0
            extra_increment = 0
            for i in range(0, self._queue.maxsize):
                self._queue.put_nowait(i)
            while True:
                if not self._queue.full():
                    now = time.monotonic()
                    increment = rate_limit * (now - updated_at)
                    fraction += increment % 1
                    extra_increment = fraction // 1
                    items_2_add = int(min(self._queue.maxsize - self._queue.qsize(), int(increment) + extra_increment))
                    fraction = fraction % 1
                    for i in range(0, items_2_add):
                        self._queue.put_nowait(i)
                    updated_at = now
                await asyncio.sleep(sleep)
        except asyncio.CancelledError:
            print('Cancelled')
        except Exception as err:
            print(str(err))

    async def _allow(self) -> None:
        """Check if a request is allowed based on the rate limit.

        This method blocks until a request is allowed according to the rate limit.
        """
        if self._queue is not None:
            # debug
            # if self._start_time == None:
            #    self._start_time = time.time()
            await self._queue.get()
            self._queue.task_done()
        return None

    async def _request(self, *args, **kwargs)  -> aiohttp.ClientResponse:
        """Perform a throttled request.

        This method overrides the parent class's _request method to implement
        rate limiting before making the actual request.

        Args:
            *args: Variable length argument list to be passed to the parent's _request method.
            **kwargs: Arbitrary keyword arguments to be passed to the parent's _request method.

        Returns:
            aiohttp.ClientResponse: The response from the request.
        """
        await self._allow()
        return await super()._request(*args, **kwargs)
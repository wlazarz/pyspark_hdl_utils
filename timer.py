import time
from typing import Optional


class Timer:
    def __init__(self):
        """
        Initializes the timer with the current time as both start and current checkpoints.
        """
        self.start_time = time.time()
        self.current_time = time.time()

    def time(self, start_time: Optional[float] = None, time_from_start: bool = False) -> str:
        """
        Returns a formatted string representing the elapsed time.

        :param start_time: Optional custom start time (timestamp) to measure from.
        :param time_from_start: If True, calculates time from initial start; otherwise from last call.
        :return: Elapsed time as formatted string (e.g., "0h1m15s250000ms").
        """
        if start_time is not None:
            elapsed = time.time() - start_time
        elif time_from_start:
            elapsed = time.time() - self.start_time
        else:
            elapsed = time.time() - self.current_time

        seconds, fractional = divmod(elapsed, 1)
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        milliseconds = int(fractional * 1_000_000)

        self.current_time = time.time()

        return f"{int(hours)}h{int(minutes)}m{int(seconds)}s{milliseconds}ms"

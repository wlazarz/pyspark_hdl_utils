import time


class Timer:

    def __init__(self):
        self.start_time = time.time()
        self.current_time = time.time()

    def time(self, start_time=None, time_from_start=False):
        if start_time is not None:
            current_time = time.time() - start_time
        else:
            if time_from_start:
                current_time = time.time() - self.start_time
            else:
                current_time = time.time() - self.current_time

        seconds, fractional_seconds = divmod(current_time, 1)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        milliseconds = int(fractional_seconds * 1000000)
        
        time_string = f"{int(hours)}h{int(minutes)}m{int(seconds)}s{milliseconds}ms"

        self.current_time = time.time()
        
        return time_string

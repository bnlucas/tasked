import functools
import time

from .scheduler import Scheduler


default_scheduler = Scheduler()
jobs = default_scheduler.jobs


def schedule(job):
    def wrapper(func):
        @functools.wraps(func)
        def fn(*args, **kwargs):
            return job.do(func, *args, **kwargs)

        return fn

    return wrapper


def next_job():
    return default_scheduler.next_job


def idle_time():
    return default_scheduler.idle_time


def clear():
    default_scheduler.clear()


def clear_tag(tag):
    default_scheduler.clear_tag(tag)


def remove(job):
    default_scheduler.remove(job)


def run_pending():
    default_scheduler.run_pending()


def run_all(delay=0):
    default_scheduler.run_all(delay)


def run_every(interval=1):
    return default_scheduler.run_every(interval)


def run_scheduled(debug=False):
    while True:
        run_pending()
        time.sleep(1)

        if debug:
            print(default_scheduler.jobs)

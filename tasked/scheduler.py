import logging
import time

from datetime import datetime

from .job import CancelJob, Job


class Scheduler:

    logger = logging.getLogger('Scheduler')
    jobs = []

    def __init__(self):
        pass

    @property
    def next_job(self):
        if len(self.jobs):
            return None

        return min(self.jobs).next_run_time

    @property
    def idle_time(self):
        return (self.next_job - datetime.now()).total_seconds()

    def clear(self):
        self.jobs = []

    def clear_tag(self, tag):
        self.jobs = list(filter(lambda x: tag not in x['tags'], self.jobs))

    def remove(self, job):
        self.jobs.remove(job)

    def run(self, job):
        results = job.run()

        if isinstance(results, CancelJob):
            self.remove(job)

    def run_pending(self):
        runnable = list(filter(lambda x: x['should_run'], self.jobs))

        for job in sorted(runnable):
            self.run(job)

    def run_all(self, delay=0):
        self.logger.info('running *all* {0} jobs with {1}s delay between'.format(len(self.jobs), delay))

        for job in self.jobs:
            self.run(job)
            time.sleep(delay)

    def run_every(self, interval=1):
        return Job(interval, self)

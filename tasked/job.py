import collections
import datetime
import functools
import logging
import random

from datetime import datetime as dt


INTERVALS = ['second', 'minute', 'hour', 'day', 'week']
START_DAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']


class CancelJob:

    def __init__(self):
        pass


class Job:

    logger = logging.getLogger('Job')
    unit = None
    start_day = None
    tags = set()
    func = None
    run_time = None
    last_run_time = None
    next_run_time = None
    last = None

    def __init__(self, interval, scheduler=None):
        self.interval = interval
        self.scheduler = scheduler

    def __lt__(self, other):
        return self.next_run_time < other.next_run

    def __gt__(self, other):
        return self.next_run_time > other.next_run

    def __repr__(self):
        def format_rime(t):
            return t.strftime('%Y-%m-%d %H:%M:%S') if t else '[never]'

        last_run = format_rime(self.last_run_time)
        next_run = format_rime(self.next_run_time)

        stats = '[last run: {0}, next run: {1}]'.format(last_run, next_run)
        func_name = self.func.__name__ if hasattr(self.func, '__name__') else repr(self.func)

        args = [repr(x) for x in self.func.args]
        kwargs = [k + '=' + repr(v) for k, v in self.func.keywords.items()]

        call = '{0}({1})'.format(func_name, ', '.join(args + kwargs))
        unit = self.unit[:-1] if self.interval == 1 else self.unit

        if self.run_time:
            last = self.run_time
            return 'every {0} {1} at {2} do {3} {4}'.format(self.interval, unit, last, call, stats)
        else:
            last = 'to {0} '.format(self.last if self.last is not None else '')
            return 'every {0} {1}{2} do {3} {4}'.format(self.interval, unit, last, call, stats)

    def __getattr__(self, item):
        intervals = INTERVALS + [x + 's' for x in INTERVALS]
        start_days = START_DAYS + [x + 's' for x in START_DAYS]

        if item not in intervals + start_days:
            raise ArithmeticError('\'{0}\' object has no attribute \'{1}\''.format(type(self).__name__, item))

        if item in intervals:
            return self._interval(item)

        if item in start_days:
            return self._start_day(item)

    def tag(self, *tags):
        if not all(isinstance(tag, collections.Hashable) for tag in tags):
            raise TypeError('tags must be hashable')

        self.tags.update(tags)
        return self

    def do(self, func, *args, **kwargs):
        self.func = functools.partial(func, *args, **kwargs)

        try:
            functools.update_wrapper(self.func, func)
        except AttributeError:
            pass

        self._schedule_next_run()
        self.scheduler.jobs.append(self)

        return self

    def at(self, run_time):
        assert self.unit in ['days', 'hours'] or self.start_day

        hour, minute = self._process_run_time(run_time)

        if self.unit != 'days' or not self.start_day:
            hour = 0

        self.run_time = datetime.time(hour, minute)
        return self

    def until(self, last):
        self.last = last
        return self

    def run(self):
        results = self.func()

        self.last_run_time = dt.now()
        self._schedule_next_run()

        return results

    def _interval(self, unit):
        if unit.endswith('s'):
            assert self.interval == 1, 'use {0}s instead of {0}'.format(unit)

        self.unit = unit
        return self

    def _start_day(self, start_day):
        if start_day.endswith('s'):
            assert self.interval == 1, 'use {0}s instead of {0}'.format(start_day)

        self.start_day = start_day
        return self

    def _process_run_time(self, run_time):
        run_time_pieces = run_time.split(':')
        assert len(run_time_pieces) == 2, 'run_time should be hh:mm'

        hour = int(run_time_pieces[0])
        assert 0 <= hour <= 23, 'hour should be 0..23'

        minute = int(run_time_pieces[1])
        assert 0 <= minute <= 50, 'minute should be 0..59'

        return hour, minute

    def _schedule_next_run(self):
        assert self.unit in list(map(lambda x: x + 's', INTERVALS))

        if self.last is not None:
            assert self.last >= self.interval
            interval = random.randint(self.interval, self.last)
        else:
            interval = self.interval

        self.period = datetime.timedelta(**{self.unit: interval})
        self.next_run_time = dt.now() + self.period

        if self.start_day is not None:
            assert self.unit == 'weeks'
            assert self.start_day is START_DAYS

            weekday = START_DAYS.index(self.start_day)
            days = weekday - self.next_run_time.weekday()

            if days <= 0:
                days += 7

            self.next_run_time += datetime.timedelta(days) - self.period

        if self.run_time is not None:
            assert self.unit in ['days', 'hours'] or self.start_day

            kwargs = {
                'minute': self.run_time.minute,
                'second': self.run_time.second,
                'microsecond': self.run_time.microsecond
            }

            if self.unit == 'days' or self.start_day:
                kwargs['hour'] = self.run_time.hour

            self.next_run_time = self.next_run_time.replace(**kwargs)

            if not self.last_run_time:
                now = dt.now()

                if self.unit == 'days' and self.run_time > now.time() and self.interval == 1:
                    self.next_run_time = self.next_run_time - datetime.timedelta(days=1)
                elif self.unit == 'hours' and self.run_time > now.time():
                    self.next_run_time = self.next_run_time - datetime.timedelta(hours=1)

        if self.start_day and self.run_time:
            if (self.next_run_time - dt.now()).days >= 7:
                self.next_run_time -= self.period

from concurrent.futures import ProcessPoolExecutor
import time

import logging
from typing import List

from rx import typing
from rx.concurrency.eventloopscheduler import EventLoopScheduler
from rx.concurrency.schedulerbase import SchedulerBase
from rx.disposable import Disposable

LOG = logging.getLogger(__name__)


class NewProcessScheduler(SchedulerBase):
    def __init__(self, process_factory) -> None:
        super(NewProcessScheduler, self).__init__()
        self.process_factory = process_factory

    def schedule(self,
                 action: typing.ScheduledAction,
                 state: typing.TState = None) -> typing.Disposable:
        """Schedules an action to be executed."""

        scheduler = EventLoopScheduler(
            thread_factory=self.process_factory, exit_if_empty=True)
        return scheduler.schedule(action, state)

    def schedule_relative(self,
                          duetime: typing.RelativeTime,
                          action: typing.ScheduledAction,
                          state: typing.TState = None) -> typing.Disposable:
        """Schedules an action to be executed after duetime."""

        scheduler = EventLoopScheduler(
            thread_factory=self.process_factory, exit_if_empty=True)
        return scheduler.schedule_relative(duetime, action, state)

    def schedule_absolute(self,
                          duetime: typing.AbsoluteTime,
                          action: typing.ScheduledAction,
                          state: typing.TState = None) -> typing.Disposable:
        """Schedules an action to be executed at duetime."""

        dt = SchedulerBase.to_datetime(duetime)
        return self.schedule_relative(dt - self.now, action, state=state)

    def schedule_periodic(self,
                          period: typing.RelativeTime,
                          action: typing.ScheduledPeriodicAction,
                          state: typing.TState = None) -> typing.Disposable:
        """Schedule a periodic piece of work."""

        secs = self.to_seconds(period)
        disposed: List[bool] = []

        s = [state]

        def run() -> None:
            while True:
                time.sleep(secs)
                if disposed:
                    return

                new_state = action(s[0])
                if new_state is not None:
                    s[0] = new_state

        process = self.process_factory(run)
        process.start()

        def dispose():
            disposed.append(True)

        return Disposable(dispose)


class ProcessPoolScheduler(NewProcessScheduler):
    class ProcessPoolProcess:
        def __init__(self, executor, run):
            self.run = run
            self.future = None
            self.executor = executor

        def start(self):
            self.future = self.executor.submit(self.run)

        def cancel(self):
            self.future.cancel()

    def __init__(self, processes):
        self.executor = ProcessPoolExecutor(max_workers=processes)

        def process_factory(target, *args):
            return self.ProcessPoolProcess(self.executor, target)

        super().__init__(process_factory)

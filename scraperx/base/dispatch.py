import sys
import math
import types
import queue
import inspect
import logging
import threading
from abc import ABC, abstractmethod

from .. import config
from ..trigger import run_task
from ..utils import rate_limited, rate_limit_from_period

logger = logging.getLogger(__name__)


class BaseDispatch(ABC):

    def __init__(self, tasks=None, download_cls=None, extract_cls=None):
        self._scraper = inspect.getmodule(self)
        self.download_cls = download_cls
        self.extract_cls = extract_cls
        self.tasks = []  # Used to be able to dump tasks to a file

        try:
            # This try/except allows the user to call super() before or after
            # self.num_tasks is set and this will not override it
            self.num_tasks
        except AttributeError:
            self.num_tasks = None

        if not tasks:
            tasks = self.submit_tasks()

        if isinstance(tasks, types.GeneratorType):
            self.tasks_generator = tasks
        else:
            if not isinstance(tasks, (list, tuple)):
                # Make sure tasks is a list and not just a single task
                tasks = [tasks]

            self.tasks = tasks
            self.num_tasks = len(tasks)
            # Create generator
            self.tasks_generator = iter(tasks)

    @abstractmethod
    def submit_tasks(self):
        """Return/yield tasks to be dispatched

        Scraper must implement this

        Returns:
            list|dict|yield -- list of tasks (dicts) or a dict of a single task
                               or a generator which yields dict's

        Decorators:
            abstractmethod
        """
        pass

    def _get_qps(self):
        """Get the rate limit from the config

        Returns:
            float -- The rate limit as qps
        """
        rate_limit_type = config['DISPATCH_RATELIMIT_TYPE']
        rate_limit_value = config['DISPATCH_RATELIMIT_VALUE']
        if rate_limit_type == 'period':
            return rate_limit_from_period(self.num_tasks, rate_limit_value)
        else:
            return rate_limit_value

    def dispatch(self, *args, **kwargs):
        """Spin up the threads to send the tasks in
        """
        if self.num_tasks is None:
            # a generator must have been passed in
            logger.critical(("Dispatch self.num_tasks must be set if using"
                             " a generator for tasks"),
                            extra={'scraper_name': config['SCRAPER_NAME'],
                                   'task': None,  # No task yet
                                   })
            sys.exit(1)

        if config['DISPATCH_LIMIT']:
            self.num_tasks = min(self.num_tasks, config['DISPATCH_LIMIT'])

        qps = self._get_qps()
        logger.info(f"Dispatch {self.num_tasks}",
                    extra={'scraper_name': config['SCRAPER_NAME'],
                           'task': None,  # No task yet
                           'qps': qps,
                           'dispatch_service': config['DISPATCH_SERVICE_NAME'],
                           'num_tasks': self.num_tasks})
        # Have 3 times the numbers of threads so a task will not bottleneck
        num_threads = math.ceil(qps * 3)
        q = queue.Queue()

        def _thread_run():
            while True:
                item = q.get()
                try:
                    self.submit_task(item, *args, **kwargs)
                except Exception:
                    logger.critical("Dispatch failed",
                                    extra={'task': item,
                                           'scraper_name': config['SCRAPER_NAME']},  # noqa E501
                                    exc_info=True)
                q.task_done()

        for i in range(num_threads):
            t = threading.Thread(target=_thread_run)
            t.daemon = True
            t.start()

        @rate_limited(num_calls=qps)
        def rate_limit_tasks():
            task = next(self.tasks_generator)
            self.tasks.append(task)
            q.put(task)

        # Fill the Queue with the data to process
        for _ in range(self.num_tasks):
            rate_limit_tasks()

        # Process the data
        q.join()

    def submit_task(self, task):
        """Send a single task off to be downloaded

        Call this and not run_task directly since this function has the
        rate limit applied to it

        Arguments:
            task {dict} -- Single task to send to the downloader
        """
        if config['DISPATCH_SERVICE_NAME'] == 'local':
            run_task(task,
                     task_cls=self.download_cls,
                     extract_cls=self.extract_cls,
                     )
        else:
            run_task(task)

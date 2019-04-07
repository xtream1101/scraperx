import math
import inspect
import logging
from abc import ABC, abstractmethod

from .. import config
from ..utils import (rate_limited, threads,
                     rate_limit_from_period)
from ..trigger import run_task

logger = logging.getLogger(__name__)


class BaseDispatch(ABC):

    def __init__(self, tasks=None, download_cls=None, extract_cls=None):
        self._scraper = inspect.getmodule(self)
        self.download_cls = download_cls
        self.extract_cls = extract_cls
        if not tasks:
            tasks = self.create_tasks()

        # Make sure tasks is a list and not just a single task
        if not isinstance(tasks, (list, tuple)):
            tasks = [tasks]
        self.tasks = tasks

        if config['DISPATCH_LIMIT']:
            self.tasks = self.tasks[:config['DISPATCH_LIMIT']]

        self.qps = self._get_qps()
        self.submit_task = rate_limited(num_calls=self.qps)(self.submit_task)

    @abstractmethod
    def create_tasks(self):
        """Generate a list of tasks to be dispatched

        Scraper must implement this

        Returns:
            list -- list of tasks (dicts)

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
            return rate_limit_from_period(len(self.tasks), rate_limit_value)
        else:
            return rate_limit_value

    def dispatch(self):
        """Spin up the threads to send the tasks in
        """
        logger.info(f"Dispatch {len(self.tasks)}",
                    extra={'scraper_name': config['SCRAPER_NAME'],
                           'task': None,  # No task yet
                           'qps': self.qps,
                           'dispatch_service': config['DISPATCH_SERVICE_NAME'],
                           'num_tasks': len(self.tasks)})
        # Have 3 times the numbers of threads so a task will not bottleneck
        num_threads = math.ceil(self.qps * 3)
        threads(num_threads,
                self.tasks,
                self.submit_task)

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

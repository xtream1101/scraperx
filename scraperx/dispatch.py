import math
import types
import queue
import logging
import threading

from .trigger import run_task
from .utils import rate_limited, rate_limit_from_period

logger = logging.getLogger(__name__)


class Dispatch():
    num_tasks = None

    def __init__(self, scraper, tasks=None, triggered_kwargs={}, **kwargs):
        self.scraper = scraper

        if not tasks:
            tasks = self.submit_tasks()

        if isinstance(tasks, types.GeneratorType):
            self.tasks_generator = tasks
            self.tasks = []
        else:
            if not isinstance(tasks, (list, tuple)):
                # Make sure tasks is a list and not just a single task
                tasks = [tasks]

            self.tasks = tasks
            self.num_tasks = len(tasks)
            # Create generator
            self.tasks_generator = iter(tasks)

    def submit_tasks(self):
        """Return/yield tasks to be dispatched

        Scraper must implement this

        Returns:
            list|dict|yield -- list of tasks (dicts) or a dict of a single task
                               or a generator which yields dict's
        """
        return []

    def _get_qps(self):
        """Get the rate limit from the config

        Returns:
            float -- The rate limit as qps
        """
        rate_limit_type = self.scraper.config['DISPATCH_RATELIMIT_TYPE']
        rate_limit_value = self.scraper.config['DISPATCH_RATELIMIT_VALUE']
        if rate_limit_type == 'period':
            return rate_limit_from_period(self.num_tasks, rate_limit_value)
        else:
            return rate_limit_value

    def run(self, **download_kwargs):
        """Spin up the threads to send the tasks in
        """
        if self.num_tasks is None:
            # a generator must have been passed in
            logger.critical(("Dispatch self.num_tasks must be set if using"
                             " a generator for tasks"),
                            extra={'scraper_name': self.scraper.config['SCRAPER_NAME'],
                                   'task': None,  # No task yet
                                   })
            raise ValueError("Dispatch.num_tasks must be set when using a generator")

        if self.scraper.config['DISPATCH_LIMIT']:
            self.num_tasks = min(self.num_tasks, self.scraper.config['DISPATCH_LIMIT'])

        qps = self._get_qps()
        logger.info(f"Dispatch {self.num_tasks}",
                    extra={'scraper_name': self.scraper.config['SCRAPER_NAME'],
                           'task': None,  # No task yet
                           'qps': qps,
                           'dispatch_service': self.scraper.config['DISPATCH_SERVICE_NAME'],
                           'num_tasks': self.num_tasks})
        # Have 3 times the numbers of threads so a task will not bottleneck
        num_threads = math.ceil(qps * 3)
        q = queue.Queue()

        def _thread_run():
            while True:
                task = q.get()
                try:
                    run_task(self.scraper, task,
                             task_cls=self.scraper.download,
                             **download_kwargs)
                except Exception:
                    logger.critical("Dispatch failed",
                                    extra={'task': task,
                                           'scraper_name': self.scraper.config['SCRAPER_NAME']},
                                    exc_info=True)
                q.task_done()

        for i in range(num_threads):
            t = threading.Thread(target=_thread_run)
            t.daemon = True
            t.start()

        @rate_limited(num_calls=qps)
        def rate_limit_tasks():
            task = next(self.tasks_generator)
            logger.debug("Adding task",
                         extra={'task': task,
                                'scraper_name': self.scraper.config['SCRAPER_NAME']})
            self.tasks.append(task)
            q.put(task)

        # Fill the Queue with the data to process
        for _ in range(self.num_tasks):
            rate_limit_tasks()

        # Process the data
        q.join()

import math
import types
import queue
import logging
import threading

from .trigger import run_task
from .utils import rate_limited, rate_limit_from_period

logger = logging.getLogger(__name__)


class Dispatch():

    def __init__(self, scraper, tasks=None, **kwargs):
        """Base Dispatch class to inherent from

        Args:
            scraper (scraperx.Scraper): The Scraper class. Scraper Will take care of
                passing itself in here
            tasks (dict|list|generator, optional): Tasks to be processed.
                Each task should be a dict. Calls submit_tasks() if None. Defaults to None.
        """
        self.scraper = scraper

        logger.info("Start Gathering Tasks...", extra={**self.scraper.log_extras()})

        if not tasks:
            tasks = self.submit_tasks()

        if isinstance(tasks, types.GeneratorType):
            self.tasks_generator = tasks
            self.tasks = []
            self.num_tasks = None
        else:
            if not isinstance(tasks, (list, tuple)):
                # Make sure tasks is a list and not just a single task
                tasks = [tasks]

            self.tasks = tasks
            self.num_tasks = len(tasks)
            # Create generator
            self.tasks_generator = iter(tasks)

    def submit_tasks(self):
        """User should override with their scrapers tasks

        Returns:
            list|dict|generator: list of tasks (dicts) or a dict of a single task
                or a generator which yields dict
        """
        return []

    def _get_qps(self):
        """Gets the queries per second from the config/cli args
        If period is set, it will convert that to the correct qps based on the number of tasks

        Returns:
            float: Queries per second to dispatch tasks at
        """
        rate_limit_type = self.scraper.config['DISPATCH_RATELIMIT_TYPE']
        rate_limit_value = self.scraper.config['DISPATCH_RATELIMIT_VALUE']
        if rate_limit_type == 'period':
            return rate_limit_from_period(self.num_tasks, rate_limit_value)
        else:
            return rate_limit_value

    def run(self, **download_kwargs):
        """Starts dispatching the tasks using threads and a local queue

        Will trigger the download for each task

        Args:
            **download_kwargs: keyword arguments to be passed into the scrapers Download class

        Raises:
            ValueError: If `self.num_tasks` is not set. Needs to be set manually if using a
                generator to submit tasks.
        """
        if self.num_tasks is None:
            logger.critical(("Dispatch self.num_tasks must be set if using"
                             " a generator for tasks"),
                            extra={**self.scraper.log_extras()})
            raise ValueError("Dispatch.num_tasks must be set when using a generator")

        if self.scraper.config['DISPATCH_LIMIT']:
            self.num_tasks = min(self.num_tasks, self.scraper.config['DISPATCH_LIMIT'])

        qps = self._get_qps()
        logger.info(f"Dispatch {self.num_tasks}",
                    extra={**self.scraper.log_extras(),
                           'qps': qps,
                           'dispatch_service': self.scraper.config['DISPATCH_SERVICE_NAME'],
                           'num_tasks': self.num_tasks})

        if self.num_tasks == 0:
            # No reason to continue
            return

        # Have 3 times the numbers of threads so a task will not bottleneck
        num_threads = math.ceil(qps * 3)
        q = queue.Queue()

        def _thread_run():
            while True:
                task = q.get()
                if task is None:
                    break

                try:
                    run_task(self.scraper, task,
                             task_cls=self.scraper.download,
                             **download_kwargs)
                except Exception:
                    logger.critical("Dispatch failed",
                                    extra={'task': task,
                                           **self.scraper.log_extras()},
                                    exc_info=True)
                task = None
                q.task_done()

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=_thread_run)
            t.daemon = True
            t.start()
            threads.append(t)

        @rate_limited(num_calls=qps)
        def _rate_limit_tasks():
            task = next(self.tasks_generator)
            logger.debug("Adding task",
                         extra={'task': task,
                                **self.scraper.log_extras()})
            self.tasks.append(task)
            q.put(task)

        # Fill the Queue with the data to process
        for _ in range(self.num_tasks):
            _rate_limit_tasks()

        # Process the data and wait until its complete
        q.join()

        # Stop and cleanup workers
        for i in range(num_threads):
            q.put(None)
        for t in threads:
            t.join()

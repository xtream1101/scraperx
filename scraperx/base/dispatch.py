import json
import math
import inspect
import logging
from abc import ABC, abstractmethod

from ..utils import (rate_limited, threads, get_scraper_config,
                     rate_limit_from_period)

logger = logging.getLogger(__name__)


class BaseDispatch(ABC):

    def __init__(self, tasks=None, cli_args=None):
        self._scraper = inspect.getmodule(self)
        self.config = get_scraper_config(self._scraper, cli_args=cli_args)

        if tasks:
            # Make sure tasks is a list and not just a single task
            if not isinstance(tasks, (list, tuple)):
                tasks = [tasks]
            self.tasks = tasks
        else:
            self.tasks = self.create_tasks()

        if self.config.get('DISPATCH_LIMIT'):
            self.tasks = self.tasks[:self.config.get('DISPATCH_LIMIT')]

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
        rate_limit_type = self.config.get('DISPATCH_RATELIMIT_TYPE')
        rate_limit_value = self.config.get('DISPATCH_RATELIMIT_VALUE')
        if rate_limit_type == 'period':
            return rate_limit_from_period(len(self.tasks), rate_limit_value)
        else:
            return rate_limit_value

    def dispatch(self, standalone=False):
        """Spin up the threads to send the tasks in

        Keyword Arguments:
            standalone {bool} -- Do not trigger the download if True
                                 (default: {False})
        """
        dispatch_service = self.config.get('DISPATCH_SERVICE_TYPE')
        logger.debug('Dispatch',
                     extra={'qps': self.qps,
                            'scraper': self._scraper.__name__,
                            'dispatch_service': dispatch_service,
                            'num_tasks': len(self.tasks)})
        # Have 3 times the numbers of threads so a task will not bottleneck
        num_threads = math.ceil(self.qps * 3)
        threads(num_threads,
                self.tasks,
                self.submit_task,
                dispatch_service,
                standalone=standalone)

    def submit_task(self, task, dispatch_service, standalone=False):
        """Send a single task off to be downloaded

        Arguments:
            task {dict} -- Single task to send to the downloader
            dispatch_service {str} -- Where to dispatch the task to

        Keyword Arguments:
            standalone {bool} -- Do not trigger the download if True
                                 (default: {False})
        """
        msg = "Dummy Trigger download" if standalone else "Trigger download"
        logger.debug(msg, extra={'dispatch_service': dispatch_service,
                                 'task': task})
        if not standalone:
            if dispatch_service == 'local':
                self._dispatch_locally(task)

            elif dispatch_service == 'sns':
                self._dispatch_sns(task)

            else:
                logger.error(f"The {dispatch_service} is not setup",
                             extra={'task': task})

    def _dispatch_locally(self, task):
        """Send the task directly to the download class

        Arguments:
            task {dict} -- Single task to be run
        """
        try:
            self._scraper.Download(task).run()

        except Exception:
            logger.critical("Local download failed",
                            extra={'task': task},
                            exc_info=True)

    def _dispatch_sns(self, task):
        """Send the task to a lambda via an SNS Topic

        Arguments:
            task {dict} -- Single task to be run
        """
        try:
            import boto3
            client = boto3.client('sns')
            target_arn = self.config.get('DISPATCH_SERVICE_SNS_ARN')
            message = {'task': task,
                       'scraper': self._scraper.__name__}
            if target_arn is not None:
                sns_message = json.dumps({'default': json.dumps(message)})
                response = client.publish(TargetArn=target_arn,
                                          Message=sns_message,
                                          MessageStructure='json'
                                          )
                logger.debug(f"SNS Response: {response}",
                             extra={'task': task})
            else:
                logger.error("Must configure sns_arn if using sns",
                             extra={'task': task})
        except Exception:
            logger.critical("Failed to dispatch sns downloader",
                            extra={'task': task},
                            exc_info=True)

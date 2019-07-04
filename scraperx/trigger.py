import json
import logging
from . import config

logger = logging.getLogger(__name__)


def run_task(task, task_cls=None, **kwargs):
    if task_cls is None:
        return

    msg = "Dummy Trigger" if config['STANDALONE'] else "Trigger"
    logger.debug(msg,
                 extra={'dispatch_service': config['DISPATCH_SERVICE_NAME'],
                        'task': task,
                        'scraper_name': config['SCRAPER_NAME']})

    if not config['STANDALONE']:
        if config['DISPATCH_SERVICE_NAME'] == 'local':
            _dispatch_locally(task, task_cls, **kwargs)

        elif config['DISPATCH_SERVICE_NAME'] == 'sns':
            _dispatch_sns(task, **kwargs)

        else:
            logger.error(f"{config['DISPATCH_SERVICE_NAME']} is not setup",
                         extra={'task': task,
                                'scraper_name': config['SCRAPER_NAME']})


def _dispatch_locally(task, task_cls, **kwargs):
    """Send the task directly to the download class

    Arguments:
        task {dict} -- Single task to be run
        task_cls {object} -- The class to init and run
    """
    from multiprocessing import Process
    try:
        if 'triggered_kwargs' in kwargs:
            del kwargs['triggered_kwargs']
        p = Process(target=task_cls(task, **kwargs, triggered_kwargs=kwargs).run)
        p.start()
    except Exception:
        logger.critical("Local task failed",
                        extra={'task': task,
                               'scraper_name': config['SCRAPER_NAME']},
                        exc_info=True)


def _dispatch_sns(task, arn=None, **kwargs):
    """Send the task to a lambda via an SNS Topic

    Arguments:
        task {dict} -- Single task to be passed along
        **kwargs {} -- All other keyword arguments passed in will be sent to
                       the SNS topic in the message

    Keyword Arguments:
        arn {str} -- ARN of the SNS topic (default: {None}, pull from config)
    """
    try:
        import boto3
        client = boto3.client('sns')
        target_arn = arn if arn else config['DISPATCH_SERVICE_SNS_ARN']
        message = {'task': task,
                   'scraper_name': config['SCRAPER_NAME'],
                   **kwargs,
                   }
        if target_arn is not None:
            sns_message = json.dumps({'default': json.dumps(message)})
            response = client.publish(TargetArn=target_arn,
                                      Message=sns_message,
                                      MessageStructure='json'
                                      )
            logger.debug(f"SNS Response: {response}",
                         extra={'task': task,
                                'scraper_name': config['SCRAPER_NAME']})
        else:
            logger.error("Must configure sns_arn if using sns",
                         extra={'task': task,
                                'scraper_name': config['SCRAPER_NAME']})
    except Exception:
        logger.critical("Failed to dispatch lambda",
                        extra={'task': task,
                               'scraper_name': config['SCRAPER_NAME']},
                        exc_info=True)

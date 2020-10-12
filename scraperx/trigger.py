import json
import logging

logger = logging.getLogger(__name__)


def run_task(scraper, task, task_cls=None, **kwargs):
    """Trigger the next step dispatch -> download -> extract

    Trigger the `task_cls` base on the config value `dispatch_service_type`

    Args:
        scraper (obj): The users Scraper instance
        task (list|dict): The task to pass to the class. Can only be list for `dispatch`
        task_cls (obj): The next part to run.
            Options are: `scraper.dispatch`, `scraper.download`, `scraper.extract`.
        **kwargs: Keyword arguments to send to pass into the `task_cls`
    """
    if task_cls is None:
        logger.warning("No tasks passed into run_task", extra={**scraper.log_extras()})
        return

    msg = "Dummy Trigger" if scraper.config['STANDALONE'] else "Trigger"
    logger.debug(msg,
                 extra={'dispatch_service': scraper.config['DISPATCH_SERVICE_NAME'],
                        'task': task,
                        **scraper.log_extras()})

    if not scraper.config['STANDALONE']:
        if scraper.config['DISPATCH_SERVICE_NAME'] == 'local':
            _dispatch_locally(scraper, task, task_cls, **kwargs)

        elif scraper.config['DISPATCH_SERVICE_NAME'] == 'sns':
            _dispatch_sns(scraper, task, **kwargs)

        else:
            logger.error(f"{scraper.config['DISPATCH_SERVICE_NAME']} is not setup",
                         extra={'task': task, **scraper.log_extras()})


def _dispatch_locally(scraper, task, task_cls, **kwargs):
    """Send the task directly to the download class"""
    if task_cls is None:
        logger.error("Cannot dispatch locally if no task class is passed in",
                     extra={'task': task, **scraper.log_extras()})
        return

    try:
        if 'triggered_kwargs' in kwargs:
            del kwargs['triggered_kwargs']
        action = task_cls(task, **kwargs, triggered_kwargs=kwargs)
        if action is None:
            # Prob the scraper does not have an extract class
            return
        # Do not run in a multi process if running locally,
        # this is to prevent the computer from getting overloaded.
        # Also this makes it so that all processes are finished before
        # returning to the users code
        action.run()
    except Exception:
        logger.critical("Local task failed",
                        extra={'task': task, **scraper.log_extras()},
                        exc_info=True)


def _dispatch_sns(scraper, task, arn=None, **kwargs):
    """Send the task to an AWS SNS Topic"""
    try:
        import boto3
        client = boto3.client('sns')
        target_arn = arn if arn else scraper.config['DISPATCH_SERVICE_SNS_ARN']
        message = {'task': task,
                   'scraper_name': scraper.config['SCRAPER_NAME'],
                   'run_id': scraper.config['RUN_ID'],
                   **kwargs,
                   }
        if target_arn is not None:
            sns_message = json.dumps({'default': json.dumps(message)})
            response = client.publish(TargetArn=target_arn,
                                      Message=sns_message,
                                      MessageStructure='json'
                                      )
            logger.debug(f"SNS Response: {response}",
                         extra={'task': task, **scraper.log_extras()})
        else:
            logger.error("Must configure sns_arn if using sns",
                         extra={'task': task, **scraper.log_extras()})
    except Exception:
        logger.critical("Failed to dispatch lambda",
                        extra={'task': task, **scraper.log_extras()},
                        exc_info=True)

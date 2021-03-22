import time
import logging

import cchardet
import threading
from smart_open import open

logger = logging.getLogger(__name__)


def get_encoding(file_bytes):
    """Guess the encoding that a byte object is encoded as

    Args:
        file_bytes (bytes, optional): Bytes to check the encoding on

    Returns:
        str: Name of the encoding used
    """
    return cchardet.detect(file_bytes)['encoding']


def read_file_contents(file_name, transport_params={}):
    # Read in file (local or s3) and check bytes for encoding type
    with open(file_name, 'rb',
              transport_params=transport_params) as f:
        raw_bytes = f.read()
    file_encoding = get_encoding(file_bytes=raw_bytes)
    # Once encoding is known, decode into correct encoding
    return raw_bytes.decode(file_encoding)


def get_root_exc_log_overides():
    """Get a dict of values from the root exception to override in the logs

    Returns:
        dict: Overide the line number and file name the python logger gets
    """
    import sys
    exception_type, exception_value, exception_traceback = sys.exc_info()
    root_exception = exception_traceback
    while hasattr(exception_traceback, 'tb_next'):
        exception_traceback = root_exception.tb_next
        if exception_traceback:
            root_exception = exception_traceback

    return {'lineno': root_exception.tb_lineno,
            'pathname': root_exception.tb_frame.f_code.co_filename,
            }


def get_context_type(context=None):
    """Check which Base class this is

    Args:
        context (obj, None): Either the Download or Extract class.
            Defaults to None.

    Returns:
        str: either 'downloader' or 'extractor'
    """
    if context is None:
        return None

    try:
        context.download
        context_type = 'downloader'
    except AttributeError:
        context_type = 'extractor'

    return context_type


def _get_s3_params(scraper, context=None, context_type=None):
    import boto3
    endpoint_url = None
    if context_type is None:
        context_type = get_context_type(context)
    endpoint_url = scraper.config[f'{context_type}_SAVE_DATA_ENDPOINT_URL']

    aws_access_key = {}
    aws_access_key_id = scraper.config[f'{context_type}_SAVE_DATA_AWS_ACCESS_KEY_ID']
    if aws_access_key_id:
        aws_access_key['aws_access_key_id'] = aws_access_key_id
    aws_secret_access_key = scraper.config[f'{context_type}_SAVE_DATA_AWS_SECRET_ACCESS_KEY']
    if aws_secret_access_key:
        aws_access_key['aws_secret_access_key'] = aws_secret_access_key

    return {
        'session': boto3.Session(**aws_access_key),
        'resource_kwargs': {
            'endpoint_url': endpoint_url,
        },
    }


def rate_limited(num_calls=1, every=1.0):
    """Rate limit a function on how often it can be called
    Source: https://github.com/tomasbasham/ratelimit/tree/0ca5a616fa6d184fa180b9ad0b6fd0cf54c46936  # noqa E501

    Args:
        num_calls (float, optional): Maximum method invocations within a period.
            Must be greater than 0. Defaults to 1
        every (float): A dampening factor (in seconds).
            Can be any number greater than 0. Defaults to 1.0

    Returns:
        function: Decorated function that will forward method invocations
            if the time window has elapsed.
    """
    frequency = abs(every) / float(num_calls)

    def decorator(func):
        """
        Extend the behavior of the following function,
        forwarding method invocations if the time window hes elapsed.

        Args:
            func (function): The function to decorate

        Returns:
            function: Decorated function
        """

        # To get around issues with function local scope
        # and reassigning variables, we wrap the time
        # within a list. When updating the value we're
        # not reassigning `last_called`, which would not
        # work, but instead reassigning the value at a
        # particular index.
        last_called = [0.0]

        # Add thread safety
        lock = threading.RLock()

        def wrapper(*args, **kwargs):
            """Decorator wrapper function"""
            with lock:
                elapsed = time.time() - last_called[0]
                left_to_wait = frequency - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                last_called[0] = time.time()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def rate_limit_from_period(num_ref_data, period):
    """Generate the QPS from a period (hrs)

    Args:
        num_ref_data (int): Number of lambda calls needed
        period (float): Number of hours to spread out the calls

    Returns:
        float: Queries per second
    """
    seconds = period * 60 * 60
    qps = num_ref_data / seconds
    return qps

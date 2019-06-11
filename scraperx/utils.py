import time
import logging
import tempfile
import threading
from .config import config

logger = logging.getLogger(__name__)


def get_file_from_s3(s3, bucket, key):
    """Download file from s3 and store in a local tmp file to be read in

    Arguments:
        s3 {boto3.resource} -- s3 resource from boto3
        bucket {str} -- Bucket the file is stored in
        key {str} -- The path of the file in the bucket

    Returns:
        str -- the name to a local tmp file
    """
    # TODO: Add support for getting metadata from the file

    tf = tempfile.NamedTemporaryFile(delete=False)
    with open(tf.name, 'w') as source_file:
        file_object = s3.Object(bucket, key)
        file_object.download_file(source_file.name)

    return tf.name


def get_context_type(context):
    """Check which Base class this is

    Arguments:
        context {class} -- Either the BaseDownload or BaseExtractor class.

    Returns:
        str -- either downloader or extractor
    """
    try:
        context.download
        context_type = 'downloader'
    except AttributeError:
        context_type = 'extractor'

    return context_type


def get_s3_resource(context):
    import boto3

    context_type = get_context_type(context)
    endpoint_url_key = f'{context_type}_SAVE_DATA_ENDPOINT_URL'
    endpoint_url = config[endpoint_url_key]

    return boto3.resource('s3', endpoint_url=endpoint_url)


def rate_limited(num_calls=1, every=1.0):
    """
    Source: https://github.com/tomasbasham/ratelimit/tree/0ca5a616fa6d184fa180b9ad0b6fd0cf54c46936  # noqa E501
    Keyword Arguments:
        num_calls {float}: Maximum method invocations within a period.
                           Must be greater than 0.
        every {float}: A dampening factor (in seconds).
                       Can be any number greater than 0.
    Return:
        function: Decorated function that will forward method invocations
                    if the time window has elapsed.
    """
    frequency = abs(every) / float(num_calls)

    def decorator(func):
        """
        Extend the behaviour of the following
        function, forwarding method invocations
        if the time window hes elapsed.
        Arguments:
            func {function}: The function to decorate

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

        def wrapper(*args, **kargs):
            """Decorator wrapper function"""
            with lock:
                elapsed = time.time() - last_called[0]
                left_to_wait = frequency - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                last_called[0] = time.time()
            return func(*args, **kargs)
        return wrapper
    return decorator


def rate_limit_from_period(num_ref_data, period):
    """Generate the QPS from a period (hrs)

    Args:
        num_ref_data {int}: Number of lambda calls needed

    Keyword Args:
        period {float}: Number of hours to spread out the calls

    Returns:
        float: Queries per second

    """
    seconds = period * 60 * 60
    qps = num_ref_data / seconds
    return qps

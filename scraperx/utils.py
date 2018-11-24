import os
import time
import queue
import logging
import tempfile
import importlib
import threading
from .config import Config

logger = logging.getLogger(__name__)


def import_scraper(lib_name):
    """Import the scraper module

    Arguments:
        lib_name {str} -- The name of the scraper. Ex: demo.search

    Returns:
        module -- The imported scraper module
    """
    logger.info('Import scraper', extra={'scraper': lib_name})
    scraper = importlib.import_module(lib_name)

    return scraper


def get_scraper_config(scraper, cli_args=None):
    """Create a config object from the scrapers config

    Arguments:
        scraper {module|str} -- The scraper to get the config from

    Keyword Arguments:
        cli_args {argparse.Namespace} -- Cli args (default: {None})

    Returns:
        class -- Config class object
    """
    if isinstance(scraper, str):
        # Need to convert it to a module if the name was passed in
        scraper = import_scraper(scraper)

    scraper_conifg = os.path.join(os.path.dirname(scraper.__file__),
                                  'config.yaml')

    scraper_name = scraper.__name__.split('.')[-1]
    config = Config(scraper_name, scraper_conifg, cli_args=cli_args)

    return config


def threads(num_threads, data, callback, *args, **kwargs):
    """Spin up threads to process a list of things

    Arguments:
        num_threads {int} -- The number of threads to create
        data {list} -- the data to interate over
        callback {function} -- The function to call
        *args {args} -- Will be passed to the callback after the data
        **kwargs {kwargs} -- Will be passed to the callback

    Returns:
        list -- List of the return values
                Order may not be the same as the input
    """
    q = queue.Queue()
    item_list = []

    def _thread_run():
        while True:
            item = q.get()
            try:
                item_list.append(callback(item, *args, **kwargs))
            except Exception:
                logger.critical("Dispatch failed",
                                extra={'task': item},
                                exc_info=True)
            q.task_done()

    for i in range(num_threads):
        t = threading.Thread(target=_thread_run)
        t.daemon = True
        t.start()

    # Fill the Queue with the data to process
    for item in data:
        q.put(item)

    # Start processing the data
    q.join()

    return item_list


def get_files_from_s3(s3, files):
    """Download files from s3

    Arguments:
        s3 {boto3.resource} -- s3 resource from boto3
        files {list} -- Files to download from s3

    Returns:
        list -- Downloaded s3 file paths
    """
    # Download the files from s3 into tmp files
    source_files = []
    for file in files:
        tf = tempfile.NamedTemporaryFile(delete=False)
        with open(tf.name, 'w') as source_file:
            bucket, key = file.split('/', 1)
            file_object = s3.Object(bucket, key)
            file_object.download_file(source_file.name)
        source_files.append(tf.name)

    return source_files


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
    try:
        endpoint_url_key = f'{context_type}_SAVE_DATA_ENDPOINT_URL'
        endpoint_url = context.config.get(endpoint_url_key)
    except ValueError:
        endpoint_url = None

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

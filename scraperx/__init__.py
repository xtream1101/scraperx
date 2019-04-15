import sys
import logging
import traceback


def uncaught(exctype, value, tb):
    logger = logging.getLogger('uncaught')
    message = ''.join(traceback.format_exception(exctype, value, tb))
    logger.critical(message, extra={'scraper_name': None, 'task': None})


sys.excepthook = uncaught

from .config import config  # noqa F401
from .run_cli import run_cli  # noqa F401

from .base.dispatch import BaseDispatch  # noqa F401
from .base.download import BaseDownload  # noqa F401
from .base.extract import BaseExtract  # noqa F401

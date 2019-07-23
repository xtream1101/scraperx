import sys
import logging
import traceback


def _uncaught(exctype, value, tb):
    logger = logging.getLogger('uncaught')
    message = ''.join(traceback.format_exception(exctype, value, tb))
    logger.critical(message, extra={'scraper_name': None, 'task': None})


sys.excepthook = _uncaught

from .run_cli import run_cli  # noqa: F401
from .trigger import run_task  # noqa: F401

from .scraper import Scraper  # noqa: F401
from .dispatch import Dispatch  # noqa: F401
from .download import Download  # noqa: F401
from .extract import Extract  # noqa: F401

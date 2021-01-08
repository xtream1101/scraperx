import sys
import logging
import traceback


def makeRecord(self, name, level, fn, lno, msg, args, exc_info,  # noqa: N802
               func=None, extra=None, sinfo=None):
    """
    A factory method which can be overridden in subclasses to create
    specialized LogRecords.
    """
    rv = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func, sinfo)
    if extra is not None:
        rv.__dict__.update(extra)
    return rv


def _uncaught(exctype, value, tb):
    logger = logging.getLogger('uncaught')
    message = ''.join(traceback.format_exception(exctype, value, tb))
    logger.critical(message, extra={'scraper_name': None, 'task': None})


logging.Logger.makeRecord = makeRecord
sys.excepthook = _uncaught

from .run_cli import run_cli  # noqa: F401, E402
from .trigger import run_task  # noqa: F401, E402

from .scraper import Scraper  # noqa: F401, E402
from .dispatch import Dispatch  # noqa: F401, E402
from .download import Download  # noqa: F401, E402
from .extract import Extract  # noqa: F401, E402

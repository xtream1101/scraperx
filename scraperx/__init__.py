import os
import sys
import logging
import traceback
import logging.config

# TODO: Does the base_dir need to be in $PATH for the
#       scraper to import local files to it?
# Test by running the scraper from an outside dir with local imports
BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
SCRAPER_NAME = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]

logging.getLogger('botocore.credentials').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'json': {
            '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': "%(asctime)s %(name)s %(levelname)s %(message)s",
        },
    },
    'handlers': {
        'default': {
            'class': 'logging.StreamHandler',
            'formatter': 'json',
            'stream': sys.stdout,
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'INFO',
            'formatter': 'json',
            'filename': f'./scraperx_{SCRAPER_NAME}_logs.json',
            'mode': 'a',
            'maxBytes': 10485760,
            'backupCount': 5,
        },
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'INFO',
            'propagate': True,
        },
    },
})


def uncaught(exctype, value, tb):
    logger = logging.getLogger('uncaught')
    message = ''.join(traceback.format_exception(exctype, value, tb))
    logger.critical(message)


sys.excepthook = uncaught


# Init the cli args and the config after the logging is setup
from .arguments import cli_args  # noqa: F401, F402
from .config import config  # noqa: F401, F402

# Now figure out what action to take
from .run import run  # noqa: F401, F402

from .base.dispatch import BaseDispatch  # noqa: F401
from .base.download import BaseDownload  # noqa: F401
from .base.extract import BaseExtract  # noqa: F401

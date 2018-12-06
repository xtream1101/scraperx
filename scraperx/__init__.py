import sys
import logging
import traceback
import logging.config


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
    },
    'loggers': {
        '': {
            'handlers': ['default'],
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

# Bases
from .base.dispatch import BaseDispatch  # noqa: F401
from .base.extract import BaseExtract  # noqa: F401
from .base.extractor import BaseExtractor  # noqa: F401
from .base.download import BaseDownload  # noqa: F401

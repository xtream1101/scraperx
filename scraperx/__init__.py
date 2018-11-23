import sys
import logging
import logging.config


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
            'level': 'DEBUG',
            'propagate': True,
        },
    },
})

# Bases
from .base.dispatch import BaseDispatch  # noqa: F401
from .base.extract import BaseExtract  # noqa: F401
from .base.extractor import BaseExtractor  # noqa: F401
from .base.download import BaseDownload  # noqa: F401

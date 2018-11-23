import logging
import inspect
import datetime
from abc import ABC, abstractmethod

from ..utils import get_scraper_config

logger = logging.getLogger(__name__)


class BaseExtract(ABC):

    def __init__(self, task, download_results):
        self._scraper = inspect.getmodule(self)
        self.config = get_scraper_config(self._scraper)
        self.task = task
        self.output = []

        self.download_results = download_results
        self.time_extracted = datetime.datetime.utcnow()
        self.date_extracted = datetime.datetime.utcnow().date()

    def run(self):
        """Run the extraction

        Triggers the `self.extract` for each source file,
        passing in the files raw contents

        User must override this fn if they want to extract the source
        files at the same time

        Use the data passed in to the __init__ to extract the data
        """
        source_files_raw = self.download_results['source_files']
        if not isinstance(source_files_raw, (list, tuple)):
            # Make a list if its not, that way either one can be passed in
            self.download_results['source_files'] = [source_files_raw]

        for source_file in self.download_results['source_files']:
            with open(source_file, 'r') as f:
                raw_source = f.read()
                self.output.append(self.extract(raw_source))
                logger.info('Extract finished', extra={'task': self.task})

    @abstractmethod
    def extract(self):
        """User created function to extract the source data

        Returns:
            list|str -- Either a list or a single extracted file

        Decorators:
            abstractmethod
        """
        pass

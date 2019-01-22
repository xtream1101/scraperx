import logging
import datetime
from abc import ABC, abstractmethod

from ..utils import get_file_from_s3, get_s3_resource

logger = logging.getLogger(__name__)


class BaseExtract(ABC):

    def __init__(self, task, download_manifest):
        self.task = task
        self.output = []

        self.download_manifest = download_manifest

        self.time_extracted = datetime.datetime.utcnow()
        self.date_extracted = self.time_extracted.date()

    def run(self):
        """Run the extraction

        Triggers the `self.extract` for each source file,
        passing in the files raw contents

        User must override this fn if they want to extract the source
        files at the same time

        Use the data passed in to the __init__ to extract the data
        """
        for source_file in self._get_sources():
            with open(source_file, 'r') as f:
                raw_source = f.read()
                self.output.append(self.extract(raw_source))

        logger.info('Extract finished', extra={'task': self.task})

    def _get_sources(self):
        """Get source files and its metadata if possiable

        Returns:
            {list} - List of tmp files saved to disk
        """
        source_files = []
        for source in self.download_manifest['source_files']:
            logger.info("Getting source file", extra={'task': self.task,
                                                      'file': source})
            if source['location'] == 's3':
                s3 = get_s3_resource(self)
                # Need to get the file from s3.
                source_files.append(get_file_from_s3(s3,
                                                     source['bucket'],
                                                     source['key']))
            elif source['location'] == 'local':
                source_files.append(source['path'])

        return source_files

    @abstractmethod
    def extract(self):
        """User created function to extract the source data

        Returns:
            list|str -- Either a list or a single extracted file

        Decorators:
            abstractmethod
        """
        pass

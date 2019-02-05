import logging
import datetime
from parsel import Selector
from abc import ABC, abstractmethod

from ..write_to import WriteTo
from ..utils import get_file_from_s3, get_s3_resource

logger = logging.getLogger(__name__)


class BaseExtract(ABC):

    def __init__(self, task, download_manifest):
        self.task = task

        self.download_manifest = download_manifest

        self.time_extracted = datetime.datetime.utcnow()
        self.date_extracted = self.time_extracted.date()

        logger.info("Start Extract",
                    extra={'task': self.task,
                           'time_started': str(self.time_extracted),
                           })

    def run(self):
        """Run the extraction

        Triggers the `self.extract` for each source file,
        passing in the files raw contents

        User must override this fn if they want to extract the source
        files at the same time

        Use the data passed in to the __init__ to extract the data

        Returns:
            list -- Files generated in the extraction process
        """
        for source_idx, source_file in enumerate(self._get_sources()):
            raw_source = None
            with open(source_file, 'r') as f:
                raw_source = f.read()

            extraction_tasks = self.extract(raw_source, source_idx)
            if not isinstance(extraction_tasks, (list, tuple)):
                extraction_tasks = [extraction_tasks]

            for extraction_task in extraction_tasks:
                if not self._validate_extraction_task(extraction_task):
                    continue

                extract_source = extraction_task.get('raw_source', raw_source)

                if not isinstance(extract_source, (list, tuple)):

                    if 'selectors' in extraction_task:
                        # It is html, so parse it out
                        parsel_source = Selector(text=extract_source)
                        # TODO: May want to only use what gets the most results?
                        #       or the first? last? Currently it will use all
                        #       selectors and get all the results
                        all_selectors = ', '.join(extraction_task['selectors'])
                        source_items = parsel_source.css(all_selectors)
                    else:
                        # Not sure what to do with the content so send it all
                        # as a single item
                        source_items = [extract_source]
                else:
                    # source is already a list
                    source_items = extract_source

                output = []
                # Used when you want to start at a different number
                offset = extraction_task.get('idx_offset', 0)
                for idx, item in enumerate(source_items, start=offset):
                    output.append(extraction_task['callback'](item, idx))

                # Save the data
                tv = extraction_task.get('file_name_vars', {})
                tv.update({'extractor_name': extraction_task.get('name', '')})
                save_as = extraction_task.get('save_as', 'json')

                if save_as == 'json':
                    WriteTo(output).write_json().save(self, template_values=tv)
                else:
                    logger.error(f"Can not save in the format `{save_as}`",
                                 extra={'task': self.task,
                                        })

        logger.info('Extract finished',
                    extra={'task': self.task,
                           'time_finished': str(datetime.datetime.utcnow()),
                           })

    def _validate_extraction_task(self, extraction_task):
        """Validate the key/values in the extraction task

        Arguments:
            extraction_task {dict} -- Passed in by the scraper

        Returns:
            bool -- If the task is valid or not
        """
        passed = True

        required_fields = ['callback', 'name']
        for field in required_fields:
            if field not in extraction_task:
                passed = False
                logger.error(f"Extraction task is missing the `{field}` field",
                             extra={'task': self.task,
                                    'extraction_task': extraction_task,
                                    })

        return passed

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

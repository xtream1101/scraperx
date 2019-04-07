import logging
import datetime
from parsel import Selector
from abc import ABC, abstractmethod

from .. import config
from ..write import Write
from ..utils import get_file_from_s3, get_s3_resource, QAValueError

logger = logging.getLogger(__name__)


class BaseExtract(ABC):

    def __init__(self, task, download_manifest):
        self.task = task

        self.download_manifest = download_manifest

        self.time_extracted = datetime.datetime.utcnow()
        self.date_extracted = self.time_extracted.date()

        logger.info("Start Extract",
                    extra={'task': self.task,
                           'scraper_name': config['SCRAPER_NAME'],
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

                try:
                    output = []
                    # Used when you want to start at a different number
                    offset = extraction_task.get('idx_offset', 0)
                    for idx, item in enumerate(source_items, start=offset):
                        result = extraction_task['callback'](item, idx)
                        if not result:
                            continue
                        # QA Result
                        # TODO: Should the QA cast to the types?
                        #       Or just make sure it is that type
                        self._qa_result(idx, extraction_task.get('qa'), result)
                        output.append(result)

                except QAValueError as e:
                    logger.error(f"Extraction Failed: {e}",
                                 extra={'task': self.task,
                                        'scraper_name': config['SCRAPER_NAME']})
                except Exception:
                    logger.exception("Extraction Failed",
                                     extra={'task': self.task,
                                            'scraper_name': config['SCRAPER_NAME']})  # noqa E501

                else:
                    post_extract = extraction_task.get('post_extract',
                                                       lambda *args, **kwargs: None)  # noqa E501
                    post_extract_kwargs = extraction_task.get('post_extract_kwargs', {})  # noqa E501
                    post_extract(output, **post_extract_kwargs)

        logger.info('Extract finished',
                    extra={'task': self.task,
                           'scraper_name': config['SCRAPER_NAME'],
                           'time_finished': str(datetime.datetime.utcnow()),
                           })

    def save_as(self, data, file_format='json', template_values={}):
        write_data = Write(data)
        save_as_map = {
            'json': write_data.write_json,
            'json_lines': write_data.write_json_lines,
        }
        if file_format not in save_as_map:
            logger.error(f"Format `{file_format}` is not supported",
                         extra={'task': self.task,
                                'scraper_name': config['SCRAPER_NAME']})

        save_as_map[file_format]().save(self, template_values=template_values)

    def _qa_result(self, idx, qa_rules, result):
        if not qa_rules:
            return

        for qa_field, qa_rule in qa_rules.items():
            # Check required
            if result[qa_field] is None:
                if qa_rule.get('required', False) is True:
                    raise QAValueError((f"Field {qa_field} is required"
                                        f" at result {idx}"))
                else:
                    # It is None and is allowed to be, so move on
                    continue

            # value_type_name also used in length check except
            value_type_name = type(result[qa_field]).__name__

            # Check Type
            if (qa_rule.get('type')
               and not isinstance(result[qa_field], qa_rule['type'])):
                err_msg = (f"Type of {qa_field} is {value_type_name}."
                           f" Expected to be of type {qa_rule['type'].__name__}"
                           f" at result {idx}")
                raise QAValueError(err_msg)

            # Check length
            try:
                if qa_rule.get('max_length') is not None:
                    if len(result[qa_field]) > qa_rule['max_length']:
                        raise QAValueError((f"Field {qa_field} is longer then"
                                            f" {qa_rule['max_length']}"
                                            f" at result {idx}"))
                if qa_rule.get('min_length') is not None:
                    if len(result[qa_field]) < qa_rule['min_length']:
                        raise QAValueError((f"Field {qa_field} is shorter then"
                                            f" {qa_rule['min_length']}"
                                            f" at result {idx}"))

            except TypeError:
                # This type of value does not support length
                logger.warning((f"The field {qa_field} of type"
                                f" {value_type_name} does not support"
                                " the length check"),
                               extra={'task': self.task,
                                      'scraper_name': config['SCRAPER_NAME']})

    def _validate_qa_rules(self, qa_rules):
        # TODO: Validate for each extraction_task in run()
        pass

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
                                    'scraper_name': config['SCRAPER_NAME'],
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
            logger.info(f"Getting source file: {source}",
                        extra={'task': self.task,
                               'scraper_name': config['SCRAPER_NAME'],
                               'file': source})
            if source['location'] == 's3':
                s3 = get_s3_resource(self)
                # Need to get the file from s3.
                source_files.append(get_file_from_s3(s3,
                                                     source['bucket'],
                                                     source['path']))
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

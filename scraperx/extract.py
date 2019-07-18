import types
import logging
import datetime
from smart_open import open
from parsel import Selector
from abc import ABC, abstractmethod

from .write import Write
from .exceptions import QAValueError
from .utils import _get_s3_params

logger = logging.getLogger(__name__)


class Extract(ABC):
    def __init__(self, scraper, task, download_manifest={}, **kwargs):
        self.scraper = scraper

        self.task = task

        self.download_manifest = download_manifest

        self.time_extracted = datetime.datetime.utcnow().isoformat() + 'Z'
        self.date_extracted = str(datetime.datetime.utcnow().date())

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
        logger.info("Start Extract",
                    extra={'task': self.task,
                           'scraper_name': self.scraper.config['SCRAPER_NAME'],
                           'time_started': self.time_extracted,
                           })

        for source_idx, source_file in enumerate(self._get_sources()):
            raw_source = None
            transport_params = {}
            if source_file.startswith('s3://'):
                transport_params = _get_s3_params(self.scraper,
                                                  context_type='downloader')

            with open(source_file, 'r', transport_params=transport_params) as f:
                raw_source = f.read()

            try:
                extraction_tasks = self._get_extraction_tasks(raw_source, source_idx)
                if not extraction_tasks:
                    continue

                for extraction_task in extraction_tasks:
                    extraction_task(raw_source)

            except Exception:
                logger.exception("Extraction Failed",
                                 extra={'task': self.task,
                                        'scraper_name': self.scraper.config['SCRAPER_NAME']})

        logger.debug('Extract finished',
                     extra={'task': self.task,
                            'scraper_name': self.scraper.config['SCRAPER_NAME'],
                            'time_finished': datetime.datetime.utcnow().isoformat() + 'Z',
                            })

    def extract_task(self, callback, name='', callback_kwargs={}, selectors=(),
                     raw_source=None, idx_offset=0, qa={}, post_extract=None,
                     post_extract_kwargs={}):
        """Create an extraction task to run on the source file

        Arguments:
            callback {function} -- The function to call on each item

        Keyword Arguments:
            name {str} -- Name of the extract task (currently not used) (default: {''})
            callback_kwargs {dict} -- Keyword arguments to pass into the callback function
                                      (default: {{}})
            selectors {tuple} -- CSS selectors used to select a list of elements that will
                                 be passed into the callback (default: {()})
            raw_source {str, list} -- Change the source content before its processed
                                      (default: {None})
            idx_offset {int} -- Starting count passed into the callback when looping over items
                                (default: {0})
            qa {dict} -- Extracted fields to qa and their rules (default: {{}})
            post_extract {function} -- Function to run on the list of outputs from the callback
                                       (default: {None})
            post_extract_kwargs {dict} -- Keyword arguments to pass into the post_extract function
                                         (default: {{}})

        Returns:
            function -- Function to pass the raw_source into for it to be processed
        """
        inputs = self._format_extract_task(locals())

        def run_extract_task(raw_source):
            if inputs.get('raw_source') is None:
                extract_source = raw_source
            else:
                extract_source = inputs['raw_source']

            if not isinstance(extract_source, (list, tuple)):
                if inputs['selectors']:
                    # It is html, so parse it out
                    parsel_source = Selector(text=extract_source)
                    source_items = self.find_css_elements(parsel_source,
                                                          inputs['selectors'])

                elif inputs['selectors'] == [] and inputs['raw_source'] is None:
                    # Want to parse the entire html source as one
                    source_items = [Selector(text=extract_source)]

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
                for idx, item in enumerate(source_items, start=inputs['idx_offset']):
                    result = inputs['callback'](item, idx, **inputs['callback_kwargs'])
                    if not result:
                        continue
                    # QA Result
                    # TODO: Should the QA cast to the types?
                    #       Or just make sure it is that type
                    self._qa_result(idx, inputs['qa'], result)
                    output.append(result)

            except QAValueError as e:
                logger.error(f"Extraction Failed: {e}",
                             extra={'task': self.task,
                                    'scraper_name': self.scraper.config['SCRAPER_NAME']})

            else:
                try:
                    inputs['post_extract'](output, **inputs['post_extract_kwargs'])
                except Exception:
                    logger.exception("Post extract Failed",
                                     extra={'task': self.task,
                                            'scraper_name': self.scraper.config['SCRAPER_NAME']})

        return run_extract_task

    def _format_extract_task(self, inputs):
        if 'self' in inputs:
            del inputs['self']

        ###
        # Callback
        ###
        if not inputs.get('callback'):
            raise ValueError("Extraction Task: callback is required")

        elif not callable(inputs['callback']):
            raise ValueError("Extraction Task: callback has to be a function")

        ###
        # Callback kwargs
        ###
        if inputs.get('callback_kwargs') is None:
            inputs['callback_kwargs'] = {}

        elif not isinstance(inputs['callback_kwargs'], dict):
            raise ValueError("Extraction Task: callback_kwargs must be dict")

        ###
        # Name
        ###
        if not isinstance(inputs['name'], (str, type(None))):
            raise ValueError("Extraction Task: name must be a string or None")

        ###
        # Selectors
        ###
        if not inputs.get('selectors'):
            inputs['selectors'] = []

        if not isinstance(inputs['selectors'], (list, tuple)):
            inputs['selectors'] = [inputs['selectors']]

        for selector in inputs['selectors']:
            if not isinstance(selector, str):
                raise ValueError("Extraction Task: All selectors must strings")

        ###
        # Raw Source
        ###

        ###
        # Index Offset
        ###
        if not inputs.get('idx_offset'):
            inputs['idx_offset'] = 0

        elif not isinstance(inputs['idx_offset'], int):
            raise ValueError("Extraction Task: idx_offset must be an integer")

        ###
        # Qa
        ###
        if inputs.get('qa') is None:
            inputs['qa'] = {}

        elif not isinstance(inputs['qa'], dict):
            raise ValueError("Extraction Task: qa must be dict")

        ###
        # Post Extract
        ###
        if not inputs.get('post_extract'):
            inputs['post_extract'] = lambda *args, **kwargs: None

        elif not callable(inputs['post_extract']):
            raise ValueError("Extraction Task: post_extract has to be a function")

        ###
        # Post Extract kwargs
        ###
        if inputs.get('post_extract_kwargs') is None:
            inputs['post_extract_kwargs'] = {}

        elif not isinstance(inputs['post_extract_kwargs'], dict):
            raise ValueError("Extraction Task: post_extract_kwargs must be dict")

        return inputs

    def _get_extraction_tasks(self, raw_source, source_idx):
        extraction_tasks = self.extract(raw_source, source_idx)
        if not extraction_tasks:
            return

        if not isinstance(extraction_tasks, (list, tuple, types.GeneratorType)):
            extraction_tasks = [extraction_tasks]

        return extraction_tasks

    def save_as(self, data, file_format='json', template_values={}):
        """Save data to a file

        Arguments:
            data {list or dict} -- Extracted data to be saved

        Keyword Arguments:
            file_format {str} -- The file format to save the data in.
                                 Options are `json` & `json_lines` (Default: json)
            template_values {dict} -- Key/Values to be used in the file_template.
                                      Gets passed along to SaveTo.save fn
        """
        write_data = Write(self.scraper, data)
        save_as_map = {
            'json': write_data.write_json,
            'json_lines': write_data.write_json_lines,
        }
        if file_format not in save_as_map:
            logger.error(f"Format `{file_format}` is not supported",
                         extra={'task': self.task,
                                'scraper_name': self.scraper.config['SCRAPER_NAME']})

        save_as_map[file_format]().save(self, template_values=template_values)

    def _qa_result(self, idx, qa_rules, result):
        if not qa_rules:
            return

        for qa_field, qa_rule in qa_rules.items():
            # Make sure key exists
            if qa_field not in result:
                raise QAValueError((f"Field {qa_field} is missing from data"
                                    f" at result {idx}"))
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
                                      'scraper_name': self.scraper.config['SCRAPER_NAME']})

    def _validate_qa_rules(self, qa_rules):
        # TODO: Validate for each extraction_task in run()
        pass

    def _get_sources(self):
        """Get source files and its metadata if possible

        Returns:
            {list} - List of tmp files saved to disk
        """
        source_files = []
        for source in self.download_manifest['source_files']:
            source_files.append(source['file'])

        return source_files

    def find_css_elements(self, source, css_selectors):
        # TODO: Add options on which selector is used (first/last/most)
        # Loop through each selector to see which ones return results,
        # Stop after the first one
        for selector in css_selectors:
            results = source.css(selector)
            if len(results) > 0:
                # Found results, save selector
                return source.css(selector)
        return []

    @abstractmethod
    def extract(self):
        """User created function to extract the source data

        Returns:
            list|str -- Either a list or a single extracted file

        Decorators:
            abstractmethod
        """
        pass

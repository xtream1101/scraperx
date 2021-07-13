import types
import logging
import datetime
from parsel import Selector
from abc import ABC, abstractmethod

from .write import Write
from .exceptions import QAValueError
from .utils import _get_s3_params, get_root_exc_log_overides, read_file_contents

logger = logging.getLogger(__name__)


class Extract(ABC):
    def __init__(self, scraper, task, download_manifest, **kwargs):
        """Base Extract class to inherent from

        Args:
            scraper (scraperx.Scraper): The Scraper class. Scraper Will take care of
                passing itself in here.
            task (dict): Task to be processed.
            download_manifest (dict): Metadata created from the Download portion of the scraper.
            **kwargs: Arbitrary keyword arguments.
        """
        self.scraper = scraper
        self.task = task
        self.download_manifest = download_manifest

        self.time_extracted = datetime.datetime.utcnow().isoformat() + 'Z'
        self.date_extracted = str(datetime.datetime.utcnow().date())

        self.pre_extract()

    def pre_extract(self):
        """User to override if any setup is needed
        """
        pass

    def run(self):
        """Starts extracting data from the source files

        Loops over each source file passing it to the users scrapers `self.extract` method.
        Passing in the source files raw content

        If all the sources need to be passed in and extracted at the same time, then the user may
        override this method to do so.
        """
        logger.info("Start Extract",
                    extra={'task': self.task,
                           **self.scraper.log_extras(),
                           'time_started': self.time_extracted,
                           })

        for source_idx, source_file in enumerate(self._get_sources()):
            if source_file.startswith('s3://'):
                transport_params = _get_s3_params(self.scraper, context_type='extractor')
            else:
                transport_params = {}
            raw_source = read_file_contents(source_file, transport_params=transport_params)

            try:
                extraction_tasks = self._get_extraction_tasks(raw_source, source_idx)
                if not extraction_tasks:
                    continue

                for extraction_task in extraction_tasks:
                    extraction_task(raw_source)

            except Exception as e:
                logger.exception(f"Extraction Failed: {e}",
                                 extra={'task': self.task,
                                        'source_file': source_file,
                                        **self.scraper.log_extras(),
                                        **get_root_exc_log_overides(),
                                        })

        logger.debug('Extract finished',
                     extra={'task': self.task,
                            **self.scraper.log_extras(),
                            'time_finished': datetime.datetime.utcnow().isoformat() + 'Z',
                            })

    def extract_task(self, callback, name='', callback_kwargs={}, selectors=(),
                     raw_source=None, idx_offset=0, qa={}, post_extract=None,
                     post_extract_kwargs={}):
        """Create an extraction task to run on the source file

        Args:
            callback (function): function to get called on each item based on the `selectors`
            name (str, optional): Name of the extract task (currently not used). Defaults to ''.
            callback_kwargs (dict, optional): Keyword arguments to pass into the callback function.
                Defaults to {}.
            selectors (tuple, optional): CSS selectors used to select a list of elements that will
                be passed into the callback. If no selectors are passed in and `raw_source` is
                not a list, then the full page source is passed in as one element. Defaults to ().
            raw_source (str|list, optional): Change the source content before its processed.
                Defaults to None.
            idx_offset (int, optional): Starting count passed into the callback when looping over
                items. Defaults to 0.
            qa (dict, optional): Extracted fields to qa and their rules. Defaults to {}.
            post_extract (function, optional): Function to run on the list of outputs from the
                callback. Defaults to None.
            post_extract_kwargs (dict, optional): Keyword arguments to pass into the post_extract
                function. Defaults to {}.

        Returns:
            function: Function to pass the raw_source into for it to be processed
        """
        inputs = self._format_extract_task(locals())

        def _run_extract_task(raw_source):
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
                    if source_items is None:
                        source_items = []

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

            try:
                inputs['post_extract'](output, **inputs['post_extract_kwargs'])
            except Exception:
                logger.exception("Post extract Failed",
                                 extra={'task': self.task,
                                        **self.scraper.log_extras()})

        return _run_extract_task

    def _format_extract_task(self, inputs):
        """Validate and foramt each argument passed into `self.extract_task`

        Args:
            inputs (dict): Arguments from `self.extract_task`

        Raises:
            ValueError: `callback` is required
            ValueError: `callback` has to be a function
            ValueError: `callback_kwargs` must be dict
            ValueError: `name` must be a string or None
            ValueError: All `selectors` must strings
            ValueError: `idx_offset` must be an integer
            ValueError: `qa` must be dict
            ValueError: `post_extract` has to be a function
            ValueError: `post_extract_kwargs` must be dict

        Returns:
            dict: The validated inputs
        """
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
        # N/A

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
        """Get the extraction tasks from the users `self.extract` method

        Args:
            raw_source (str|bytes): Raw content of the source file to extract
            source_idx (int): The index of the source file that was downloaded

        Returns:
            list: Extraction task functions to run on the raw_source
        """
        extraction_tasks = self.extract(raw_source, source_idx)
        if not extraction_tasks:
            return

        if not isinstance(extraction_tasks, (list, tuple, types.GeneratorType)):
            extraction_tasks = [extraction_tasks]

        return extraction_tasks

    def save_as(self, data, file_format='json', template_values={}):
        """Save the extracted data to a file

        Args:
            data (list): List of dicts to be saved
            file_format (str, optional): File type to save data to.
                Current options are `json` & `json_lines`. Defaults to 'json'.
            template_values (dict, optional): Key/values used when createing the file name from
                the `file_template` from the config yaml. Defaults to {}.
        """
        write_data = Write(self.scraper, data)
        save_as_map = {
            'json': write_data.write_json,
            'json_lines': write_data.write_json_lines,
        }
        if file_format not in save_as_map:
            logger.critical(f"Format `{file_format}` is not supported",
                            extra={'task': self.task,
                                   **self.scraper.log_extras()})
        else:
            save_as_map[file_format]().save(self, template_values=template_values)

    def _qa_result(self, idx, qa_rules, result):
        """QA the data as it gets extracted

        Args:
            idx (int): index of the item being extracted
            qa_rules (dict): QA Rules set in the scrapers self.extract_task
            result (dict): Single row of data that was extracted

        Raises:
            QAValueError: Filed is missing in result
            QAValueError: Field is required to have a value
            QAValueError: Type of result field does not match
            QAValueError: Result field is to long
            QAValueError: Result field is to short
        """
        if not qa_rules:
            return

        for qa_field, qa_rule in qa_rules.items():
            # Make sure key exists
            if qa_field not in result:
                if 'default' in qa_rule:
                    result[qa_field] = qa_rule['default']
                    # No need to check other params since this was user set
                    continue
                else:
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
                                      **self.scraper.log_extras()})

    def _validate_qa_rules(self, qa_rules):
        # TODO: Validate for each extraction_task in run()
        pass

    def _get_sources(self):
        """Gets a list of source filed from the download_manifest

        Returns:
            list: Paths of source files
        """
        source_files = []
        for source in self.download_manifest['source_files']:
            source_files.append(source['file'])

        return source_files

    def find_css_elements(self, source, css_selectors):
        """Find a element from a list of css selectors

        TODO: Add options on which selector is used (first/last/most)

        Given a list of css selectors, this will loop through the selectors and
        the first one to return results will return the selected elements.

        Args:
            source (Parsel object): A Parsel html element
            css_selectors (list): List of css selectors to try to find an element

        Returns:
            list: Parsel elements if any are found, else an empty list
        """
        for selector in css_selectors:
            results = source.css(selector)
            if len(results) > 0:
                # Found results, save selector
                return source.css(selector)
        return []

    @abstractmethod
    def extract(self, raw_source, source_idx):
        """User created function to extract the source data

        Args:
            raw_source (str|bytes): Raw content of the source file to extract
            source_idx (int): The index of the source file that was downloaded

        Yields:
            function: `self.extract_task()` based on the users extraction needs
        """
        pass

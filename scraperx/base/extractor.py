import json
import logging
from parsel import Selector
from abc import ABC, abstractmethod

from ..write_to import WriteTo
from ..utils import QAValueError

logger = logging.getLogger(__name__)


class BaseExtractor(WriteTo, ABC):

    def __init__(self, context, raw_source, auto_parse=True, page_type='html'):
        self.context = context  # TODO: Find a way to not pass this around
        self.results = []  # Expect this data to be a list of dicts to be saved
        super().__init__(self.results)

        if auto_parse is True:
            if page_type == 'html':
                self.source = Selector(text=raw_source)
            elif page_type == 'json':
                self.source = json.loads(raw_source)
            else:
                raise NotImplementedError((f"Page type '{page_type}' "
                                           "is not yet supported"))
        else:
            self.source = raw_source

    def extract_results(self, result_selectors=None, results=None):
        """Find and loop through the page results

        Keyword Arguments:
            result_selectors {list} -- css selectors used to find results
                                       (default: {None})
            results {list} -- list of items to send to extract_result
                              (default: {None})

        Raises:
            ValueError -- Only pass in result_selectors OR results, not both
        """
        # TODO: May want to only use what gets the most results?
        #       or the first? last? Currently it will use all
        #       selectors and get all the results
        if results is None and result_selectors is not None:
            items = self.source.css(', '.join(result_selectors))

        elif results is not None and result_selectors is None:
            items = results

        else:
            raise ValueError(("Must pass in only one: "
                              "`result_selectors` OR `results`"))

        for idx, item in enumerate(items, start=1):
            result = self.extract_result(idx, item)
            self.qa_result(result)
            self.results.append(result)

    def qa_result(self, result):
        """Use the scrapers data_type values if it has them set to
           confirm the data types of the values

        Arguments:
            result {dict} -- Extracted data for the element that was extracted
        """
        try:
            for field_name, data_type in self.data_types.items():
                if not isinstance(data_type, (list, tuple)):
                    data_type = (data_type,)

                field_value = result.get(field_name)
                field_type = type(field_value)
                if field_type not in data_type:
                    extra = {'task': self.context.task,
                             'field': {'value': field_name,
                                       'type': field_type,
                                       'expected_type': data_type,
                                       }
                             }
                    logger.critical("Invalid type for field", extra=extra)
                    raise QAValueError()

        except AttributeError:
            # Scraper does not have data_types class variable set
            pass

    @abstractmethod
    def extract_result(self, element):
        pass

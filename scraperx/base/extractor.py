import json
from parsel import Selector
from abc import ABC, abstractmethod

from ..write_to import WriteTo


class BaseExtractor(WriteTo, ABC):

    def __init__(self, raw_source, auto_parse=True, page_type='html'):
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
            self.results.append(self.extract_result(idx, item))

    @abstractmethod
    def extract_result(self, element):
        pass

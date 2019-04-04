from scraperx import run_cli, BaseDispatch, BaseDownload, BaseExtract
from scraperx.write import Write


class Dispatch(BaseDispatch):

    def create_tasks(self):
        """Returns a single or a list of tasks to be downloaded

        The keys in the task can be anything that you want to be passed along
        to be used at different steps in the scraper. There are a few built-in
        keys that will be used behind the scenes if set.

        Built-ins:

            headers {dict} --
                Custom headers to use when downloading. If no `User-Agent` is
                set one will be set automatically to a random User-Agent based
                on the `device_type` below.

            proxy {str} --
                Use a specific proxy for this request. If not set, a random
                proxy will be used from the `PROXY_FILE` if one was set.

            proxy_country {str} --
                2 letter country code. Used when selecting a proxy from
                `PROXY_FILE` if set.

            device_type {str} --
                Options are `desktop` or `mobile`. Default is `desktop`.
                If no `User-Agent` is set in the headers key, this will be
                used when selecting the type of User-Agent to use.

        """
        return {'url': 'http://testing-ground.scraping.pro/blocks'}


class Download(BaseDownload):
    """Make all requests and save any source files needed to be extracted

    BaseDownload class gives access to:

    self.task {dict} --
        A single task that was dispatched

    self.time_downloaded {datetime.datetime} --
        UTC timestamp

    self.date_downloaded {datetime.date} --
        UTC date

    self.session {requests.sessions.Session} --
        Requests session that will be used for all requests

    self.request_get {function} --
        Calls a requests.get using the session. It also adds the kwarg
        `max_tries` which defaults to 3. All other args/kwargs get passed
        to the requests.get method. Any headers passed in will be merged
        with the session headers. This will return the requests response.

    """

    def download(self):
        """Download the source files based on the task

        1. Make the request(s)
        2. Save sources to files
        3. Return a list of saved sources

        Returns:
            list/dict -- List/dict of output(s) from the .save() fn
        """
        r = self.request_get(self.task['url'])

        return Write(r.text).write_file().save(self)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        """Returns a single or a list of extractors

        In each extractor that is returned, the possiable fields are:

        Required:
            name {str} --
                Used to define the extractor.

            selectors {list of str} --
                List of css selectors to use to select the content to pass to
                the extractor. If parsing non html content and `raw_source`
                is set, then this is NOT required.

            callback {function} --
                The extract method that will be called for each element
                that the selectors find.

        Optional:
            raw_source {str} --
                If source is `html`:
                    Used if you need to modify the source before the selectors
                    are run on it. Normally not needed.
                If source is not `html`, i.e. json/plain text/...:
                    In this case do NOT pass `selectors', but set `raw_source`
                    to a list of things to be passed to the `callback`.

            idx_offset {int} --
                The index of the item that the `callback` is currently
                processing. If not set, it will start at 0.

            post_extract {function} --
                Function to call after that extractor is finished. Will pass
                the list of data as the first argument.

            post_extract_kwargs {dict} --
                Keyword arguments to send to the `post_extract` function

            qa {dict} -- TODO: link to qa docs.

        Arguments:
            raw_source {str} -- Content from the source file.
            source_idx {int} -- Index of the source file that was downloaded.

        """
        return {'name': 'products',
                'selectors': ['#case1 > div:not(.ads)'],
                'callback': self.extract_product,
                'post_extract': self.save_as,  # TODO: add docs about what builtin's there are
                'post_extract_kwargs': {'file_format': 'json',
                                        },
                }

    def extract_product(self, element, idx, **kwargs):
        return {'title': element.css('div.name').xpath('string()').extract_first()}


if __name__ == '__main__':
    import logging
    from pythonjsonlogger import jsonlogger

    logging.getLogger('botocore.credentials').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    logHandler = logging.StreamHandler()
    logHandler.setFormatter(jsonlogger.JsonFormatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
    root_logger.addHandler(logHandler)

    run_cli(Dispatch, Download, Extract)

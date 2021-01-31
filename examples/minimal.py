from scraperx import Scraper, run_cli, Dispatch, Download, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
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
        return {'url': 'https://www.imdb.com/chart/top/'}


class MyDownload(Download):
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
        # `r` is a python requests response
        r = self.request_get(self.task['url'])

        # Save the response contents to a file as set in
        # the config `DOWNLOADER_FILE_TEMPLATE`
        self.save_request(r)


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        """Returns a single or a list of extractors

        Arguments:
            raw_source {str} -- Content from the source file.
            source_idx {int} -- Index of the source file that was downloaded.
        """

        # For the self.extract_task fn
        # Arguments:
        #     callback {function} -- The function to call on each item

        # Keyword Arguments:
        #     name {str} -- Name of the extract task (currently not used) (default: {''})
        #     callback_kwargs {dict} -- Keyword arguments to pass into the callback function
        #                               (default: {{}})
        #     selectors {tuple} -- CSS selectors used to select a list of elements that will
        #                          be passed into the callback (default: {()})
        #     raw_source {str, list} -- Change the source content before its processed
        #                               (default: {None})
        #     idx_offset {int} -- Starting count passed into the callback when looping over items
        #                         (default: {0})
        #     qa {dict} -- Extracted fields to qa and their rules (default: {{}})
        #     post_extract {function} -- Function to run on the list of outputs from the callback
        #                                (default: {None})
        #     post_extract_kwargs {dict} -- Keyword arguments to pass into the post_extract function
        #                                  (default: {{}})

        # Either yield or return a list of the functions
        yield self.extract_task(
            name='products',
            selectors=['tbody.lister-list tr'],
            callback=self.extract_product,
            post_extract=self.save_as,  # TODO: add docs about what builtin's there are
            post_extract_kwargs={'file_format': 'json'},
        )

    def extract_product(self, element, idx, **kwargs):
        return {
            'title': element.css('td.titleColumn a').xpath('string()').extract_first(),
            'year': int(element.css('span.secondaryInfo').xpath('string()').extract_first()[1:-1]),
            'rating': float(element.css('td.ratingColumn ').xpath('string()').extract_first()),
        }


# `extract_cls` is the only required argument,
# the others have defaults if there is nothing custom to add
my_scraper = Scraper(dispatch_cls=MyDispatch,
                     download_cls=MyDownload,
                     extract_cls=MyExtract)

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s'
    )
    run_cli(my_scraper)

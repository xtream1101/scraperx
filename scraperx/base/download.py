import json
import inspect
import logging
import datetime
import requests
from abc import ABC, abstractmethod

from ..write_to import WriteTo
from ..save_to import SaveTo
from ..proxies import get_proxy
from ..user_agent import get_user_agent
from ..utils import get_scraper_config

logger = logging.getLogger(__name__)


class BaseDownload(ABC):

    def __init__(self, task, cli_args=None, cookies=None, headers=None,
                 proxy=None, ignore_codes=[]):
        # General task and config setup
        self._scraper = inspect.getmodule(self)
        self.config = get_scraper_config(self._scraper, cli_args=cli_args)
        self.task = task
        self.ignore_codes = ignore_codes

        # Needed so it can be passed to the extractor when running locally
        self.cli_args = cli_args

        # Set timestamps
        self.time_downloaded = datetime.datetime.utcnow()
        self.date_downloaded = datetime.datetime.utcnow().date()

        # Set up a requests session
        self.session = requests.Session()

        self._init_headers(headers)
        self._init_cookies(cookies)
        self._init_proxy(proxy)
        self._init_http_methods()

    @abstractmethod
    def download(self):
        """User created download function

        Returns:
            list|str -- Either a list or a single downloaded file

        Decorators:
            abstractmethod
        """
        pass

    def _get_proxy(self, alpha2=None, platform=None):
        """Get a proxy to use

        Use the scrapers fn, otherwise use the default

        Keyword Arguments:
            alpha2 {str} -- 2 letter country code (default: {None})
            platform {str} -- The name of the platform. Ex: Google, Macys, etc
                              (default: {None})

        Returns:
            str -- proxy string
        """
        try:
            return self.get_proxy(alpha2=None, platform=None)
        except AttributeError:
            return get_proxy(alpha2=None, platform=None)

    def _get_user_agent(self, device_type):
        """Get a User Agent

        Use the scrapers fn, otherwise use the default

        Arguments:
            device_type {str} -- The device the user agent should be for.
                                 Ex: desktop, mobile

        Returns:
            str -- User Agent string
        """
        try:
            return self.get_user_agent(device_type)
        except AttributeError:
            return get_user_agent(device_type)

    def run(self, standalone=False):
        """Start the download process

        Keyword Arguments:
            standalone {bool} -- Do not trigger the extractor if True
                                 (default: {False})
        """
        try:
            source_files = self.download()

        except requests.exceptions.HTTPError as e:
            failed_status_code = e.response.status_code
            logger.error(f"Download Error: {e}",
                         extra={'task': self.task,
                                'status_code': failed_status_code})
        else:
            if source_files:
                self._run_success(source_files, standalone)
            else:
                logger.warning("No source file saved",
                               extra={'task': self.task})

    def _run_success(self, source_files, standalone):
        """Download was successful

        Save the metadata and dispatch the extractor

        Arguments:
            source_files {list} -- The paths of the downloaded files
            standalone {bool} -- Do not trigger the extractor if True
        """
        # Make sure source files are a list
        if not isinstance(source_files, (list, tuple)):
            source_files = [source_files]

        download_manifest = {'source_files': source_files,
                             # Date/times need to be strings since they
                             #   need to be converted to json to be saved
                             'time_downloaded': str(self.time_downloaded),
                             'date_downloaded': str(self.date_downloaded),
                             }

        save_service = self.config.get(f'DOWNLOADER_SAVE_DATA_SERVICE')
        if save_service in ['local']:
            # Save the metadata file next to the source since it cannot
            #   write the data into the file itself
            self._save_metadata(download_manifest)

        run_task_on = self.config.get('DISPATCH_SERVICE_TYPE')
        msg = "Dummy Trigger extract" if standalone else "Trigger extract"
        logger.debug(msg, extra={'run_task_on': run_task_on,
                                 'task': self.task})

        if not standalone:
            try:
                self._scraper.Extract
            except AttributeError:
                logger.info("Scraper has no extract",
                            extra={'task': self.task})
            else:
                if run_task_on == 'local':
                    self._dispatch_locally(download_manifest)

                elif run_task_on == 'lambda':
                    self._dispatch_lambda(download_manifest)

                else:
                    logger.critical(f"{run_task_on} is not supported",
                                    extra={'task': self.task})

    def _get_metadata(self, download_manifest):
        """Create the metadata dict

        Arguments:
            download_manifest {dict} -- The downloads manifest

        Returns:
            {dict} -- metadata
        """
        metadata = {'task': self.task,
                    'scraper': self._scraper.__name__,
                    'download_manifest': download_manifest}
        return metadata

    def _save_metadata(self, download_manifest):
        """Save the metadata with the download source

        Saves a file as the same name as the source with '.metadata.json'
        appended to the name

        Arguments:
            download_manifest {dict} -- The downloads manifest
        """
        metadata = self._get_metadata(download_manifest)
        metadata_file = WriteTo(metadata).write_json()
        filename = download_manifest['source_files'][0]['path']
        logger.info("Saving metadata file", extra={'task': self.task})
        metadata_file.save(self, filename=filename + '.metadata.json')

    def _dispatch_lambda(self, download_manifest):
        """Send the task to a lambda via an SNS Topic

        Arguments:
            download_manifest {dict} -- The downloads manifest
        """
        try:
            import boto3
            client = boto3.client('sns')
            target_arn = self.config.get('DISPATCH_SERVICE_SNS_ARN')
            message = self._get_metadata(download_manifest)
            if target_arn is not None:
                sns_message = json.dumps({'default': json.dumps(message)})
                response = client.publish(TargetArn=target_arn,
                                          Message=sns_message,
                                          MessageStructure='json'
                                          )
                logger.debug(f"SNS Response: {response}",
                             extra={'task': self.task})
            else:
                logger.error("Must configure sns_arn when running in lambda",
                             extra={'task': self.task})
        except Exception:
            logger.critical("Failed to dispatch lambda extractor",
                            extra={'task': self.task},
                            exc_info=True)

    def _dispatch_locally(self, download_manifest):
        """Send the task directly to the download class

        Arguments:
            download_manifest {dict} -- The downloads manifest
        """
        try:
            self._scraper.Extract(self.task,
                                  download_manifest,
                                  cli_args=self.cli_args,
                                  ).run()
        except Exception:
            logger.critical("Local extract failed",
                            extra={'task': self.task},
                            exc_info=True)

    def _format_proxy(self, proxy):
        """Convert the proxy string into a dict the way requests likes it

        Arguments:
            proxy {str} -- Proxy string

        Returns:
            dict -- Format that requests wants proxies in
        """
        logger.debug(f"Setting proxy {proxy}", extra={'task': self.task})
        if isinstance(proxy, dict) and 'http' in proxy and 'https' in proxy:
            # Nothing more to do
            return proxy

        return {'http': proxy,
                'https': proxy
                }

    def _init_headers(self, headers):
        """Set up the default session headers

        If no user agent is set then a default one is set

        Arguments:
            headers {dict} -- Headers passed in to the __init__
        """
        # Set headers from init, then update with task headers
        self.session.headers = {} if headers is None else headers
        self.session.headers.update(self.task.get('headers', {}))
        # Set a UA if the scraper did not set one
        if 'user-agent' not in map(str.lower, self.session.headers.keys()):
            self._set_session_ua()

    def _init_cookies(self, cookies):
        """Set up the default session cookies

        Arguments:
            cookies {dict} -- Cookies passed in to the __init__
        """
        # Set cookies from init, then update with task cookies
        self.session.cookies.update({} if cookies is None else cookies)
        self.session.cookies.update(self.task.get('cookies', {}))

    def _init_proxy(self, proxy):
        """Set the default session proxy

        If no proxy is passed in to __init__ or in the task data,
        then set one using the task `geo_alpha2` and/or `platfrom` keys.
        If they are not set then a random proxy will be choosen

        Arguments:
            proxy {str} -- Proxy passed in to the __init__
        """
        proxy_str = proxy
        if self.task.get('proxy') is not None:
            proxy_str = self.task.get('proxy')
        # If no proxy has been passed in, try and set one
        if not proxy_str:
            proxy_str = self._get_proxy(alpha2=self.task.get('geo_alpha2'),
                                        platform=self.task.get('platform'))
        self.session.proxies = self._format_proxy(proxy_str)

    def _init_http_methods(self):
        """Generate functions for each http method

        Makes it simpler to use
        """
        # Create http methods
        self.get = self._set_http_method('GET')
        self.post = self._set_http_method('POST')
        # Not sure if these are needed, but it doesn't hurt to have them
        self.head = self._set_http_method('HEAD')
        self.put = self._set_http_method('PUT')
        self.patch = self._set_http_method('PATCH')
        self.delete = self._set_http_method('DELETE')

    def _set_http_method(self, http_method):
        """Closure for creating the http method functions

        Arguments:
            http_method {str} -- Method to return a function for

        Returns:
            function -- the Closure

        Raises:
            ValueError -- If the max number of attempts have been met
        """
        def make_request(url, max_tries=3, _try_count=1, **kwargs):
            if max_tries < 1:
                # TODO: Find a better error to raise
                raise ValueError("max_tries must be >= 1")

            if 'proxy' in kwargs:
                # Proxy is not a valid arg t pass in, so fix it
                kwargs['proxies'] = self._format_proxy(kwargs['proxy'])
                del kwargs['proxy']
            elif 'proxies' in kwargs:
                # Make sure they are in the correct format
                kwargs['proxies'] = self._format_proxy(kwargs['proxies'])

            r = self.session.request(http_method, url, **kwargs)

            logger.info(f"{http_method} request finished",
                        extra={'url': url,
                               'try_count': _try_count,
                               'max_tries': max_tries,
                               'task': self.task})

            if r.status_code != requests.codes.ok:
                if r.status_code in self.ignore_codes:
                    return Request(r)
                elif _try_count < max_tries:
                    kwargs = self.new_profile(**kwargs)
                    request_method = self._set_http_method(http_method)
                    return request_method(url,
                                          max_tries=max_tries,
                                          _try_count=_try_count + 1,
                                          **kwargs)
                else:
                    r.raise_for_status()

            return Request(r)

        return make_request

    def get_file(self, url, **kwargs):
        r = self.session.get(url, stream=True, **kwargs)
        return Request(r, r.content)

    def _set_session_ua(self):
        """Set up the session user agent

        Try and set a default user agent for the session
        """
        device_type = self.task.get('device_type', 'desktop')
        try:
            ua = self._get_user_agent(device_type)
            self.session.headers.update({'user-agent': ua})
        except ValueError:
            logger.error("Invalid device type {device_type} for UA")

    def new_profile(self, **kwargs):
        """Set a new user agent and proxy to be used for the request

        Arguments:
            **kwargs {kwargs} -- Used when only changing the request,
                                 not the session

        Returns:
            kwargs -- The args for the new request
        """
        # Set new UA
        # TODO: make this for headers in general
        #       this sdk will only update the UA
        #       let the scraper update more
        self._set_session_ua()

        # TODO: Add option to update cookies
        #       This sdk will not change any cookies
        #       let the scraper update if needed

        # Set new proxy
        proxy_str = self._get_proxy(alpha2=self.task.get('geo_alpha2'),
                                    platform=self.task.get('platform'))
        if 'proxy' in kwargs:
            # Replace the request specific
            kwargs['proxy'] = proxy_str
        elif 'proxies' in kwargs:
            # Replace the request specific
            kwargs['proxies'] = proxy_str
        else:
            # Replace the session proxy
            self.session.proxies = self._format_proxy(proxy_str)

        return kwargs


class Request(WriteTo):

    def __init__(self, request, source=None):
        self.r = request
        if source:
            self.source = source
        else:
            self.source = self.r.text
        super().__init__(self.source)


class File(SaveTo):

    def __init__(self, request, data):
        self.r = request
        self.data = data
        super().__init__(self.data)

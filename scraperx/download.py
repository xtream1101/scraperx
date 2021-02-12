import re
import logging
import datetime
import requests

from .write import Write
from .trigger import run_task
from .proxies import get_proxy
from .user_agent import get_user_agent
from .exceptions import DownloadValueError, HTTPIgnoreCodeError

logger = logging.getLogger(__name__)


class Download:
    def __init__(self, scraper, task, headers=None, proxy=None, ignore_codes=(),
                 triggered_kwargs={}, **kwargs):
        """Base Download class to inherent from

        Args:
            scraper (scraperx.Scraper): The Scraper class. Scraper Will take care of
                passing itself in here.
            tasks (dict): Task to be processed.
            headers (dict, optional): Headers to be set for all requests. Defaults to None.
            proxy (str, optional): Full url of a proxy to use for all requests. Defaults to None.
            ignore_codes (tuple, optional): Tuple of http status codes to not re-try on.
                Defaults to ().
            triggered_kwargs (dict, optional): Dict of keyword arguments to pass into the
                scrapers Extract class. Defaults to {}.
            **kwargs: Arbitrary keyword arguments.
        """
        self.scraper = scraper
        self._triggered_kwargs = triggered_kwargs

        self.task = task
        self._ignore_codes = ignore_codes

        # Set timestamps
        self.time_downloaded = datetime.datetime.utcnow().isoformat() + 'Z'
        self.date_downloaded = str(datetime.datetime.utcnow().date())

        logger.info("Start Download",
                    extra={'task': self.task,
                           **self.scraper.log_extras(),
                           'time_started': self.time_downloaded,
                           })

        self._manifest = {'source_files': [],
                          'time_downloaded': self.time_downloaded,
                          'date_downloaded': self.date_downloaded,
                          }

        # Set up a requests session
        self.session = requests.Session()

        self._init_headers(headers)
        self._init_proxy(proxy)
        self._init_http_methods()

    def download(self):
        """Scrapers download class should override this method if more then the default is needed.
        The default download method does::

            r = self.request_get(self.task['url'])
            self.save_request(r)

        """
        r = self.request_get(self.task['url'])
        self.save_request(r)

    def _get_proxy(self, country=None):
        """Get a new proxy to use for a request
        Users scraper should override this if a custom way to get a proxy is needed.
        The default way can be found under `scraperx.proxy.get_proxy`

        Args:
            country (str, optional): 2 letter country code to get the proxy for.
                If None it will get any proxy. Defaults to None.

        Returns:
            str|None: Full url of the proxy or None if no proxy is available
        """
        try:
            return self.get_proxy(self.scraper, country=country)
        except AttributeError:
            return get_proxy(self.scraper, country=country)

    def _get_user_agent(self, device_type):
        """Get a new user-agent to use for a request
        Users scraper should override this if a custom way to get a user-agent is needed.
        The default way can be found under `scraperx.user_agent.get_user_agent`

        Args:
            device_type (str): A way to pick the correct user-agent.
                The options for the default function is `desktop` or `mobile`

        Returns:
            str: A User-Agent string
        """
        try:
            return self.get_user_agent(self.scraper, device_type)
        except AttributeError:
            return get_user_agent(self.scraper, device_type)

    def run(self):
        """Starts downloading data based on the task

        Will trigger the extract task after its complete
        """
        try:
            self.download()
        except (requests.exceptions.HTTPError, HTTPIgnoreCodeError):
            # The status code was logged during the request, no need to repeat
            pass
        except DownloadValueError:
            # The status code was logged during the request, no need to repeat
            pass
        except Exception:
            logger.exception("Download Exception",
                             extra={'task': self.task,
                                    **self.scraper.log_extras()})
        else:
            if self._manifest['source_files']:
                self._save_metadata()
                run_task(self.scraper,
                         self.task,
                         task_cls=self.scraper.extract,
                         download_manifest=self._manifest,
                         **self._triggered_kwargs,
                         triggered_kwargs=self._triggered_kwargs)
            else:
                # If it got here and there is not saved file then thats an issue
                logger.error("No source file saved",
                             extra={'task': self.task,
                                    **self.scraper.log_extras(),
                                    'manifest': self._manifest,
                                    })

        logger.debug('Download finished',
                     extra={'task': self.task,
                            **self.scraper.log_extras(),
                            'time_finished': datetime.datetime.utcnow().isoformat() + 'Z',
                            })

    def save_request(self, r, content=None, source_file=None, content_type=None, **save_kwargs):
        """Save the data from the request into a file and save the request data in the metadata file
        This is needed to pass the source file into the extract class

        Args:
            r (requests.request): Response from the requests lib
            content (str|bytes, optional): The data of the source to be saved.
                If saving a binary file, set to r.contents.
                Defaults to r.text.
            source_file (str, optional): Path to a saved file if already saved. Defaults to None.
            content_type (str, optional): Mimetype of the file.
                If None, a best guess will be made. Defaults to None.
            **saved_kwargs: Keyword arguments that will be passed into
                `scraperx.save_to.SaveTo.save` function

        Returns:
            str: Path to the source file that was saved
        """
        if content is None:
            content = r.text

        if source_file is None:
            source_file = Write(self.scraper, content, encoding=r.encoding)\
                .write_file(content_type=content_type)\
                .save(self, **save_kwargs)

        self._manifest['source_files'].append(
            {
                'file': source_file,
                'request': {
                    'url': r.url,
                    'method': r.request.method,
                    'status_code': r.status_code,
                    'headers': {
                        'request': dict(r.request.headers),
                        'response': dict(r.headers),
                    },
                },
            }
        )

        return source_file

    def _save_metadata(self):
        """Save the metadata of the download portion of the scraper to a json file.
        This is used to pass to the extract class as well as debugging if
        anything goes wrong with the request.

        Saves a file ending in `_metadata.json` in the same path as the first source file saved.
        """
        if self.scraper.config['DOWNLOADER_SAVE_METADATA']:
            metadata = self._get_metadata()
            if metadata['download_manifest']['source_files']:
                metadata_file = Write(self.scraper, metadata).write_json_lines()
                filename = metadata['download_manifest']['source_files'][0]['file']
                logger.debug("Saving metadata file",
                             extra={'task': self.task,
                                    **self.scraper.log_extras()})
                metadata_file.save(self, filename=filename + '_metadata.json')

    def _get_metadata(self):
        """Create the dict of metadata to be saved

        Returns:
            dict: Data to be saved in the metadata file
        """
        metadata = {'task': self.task,
                    'scraper_name': self.scraper.config['SCRAPER_NAME'],
                    'run_id': self.scraper.config['RUN_ID'],
                    'download_manifest': self._manifest,
                    }
        return metadata

    def _format_proxy(self, proxy):
        """Format the proxy in a way the requests lib can handle

        Args:
            proxy (str|dict): Full url of the proxy or a dict that the
                requests library can use when setting a proxy

        Returns:
            dict: This is what the requests library uses when setting a proxy
        """
        logger.debug(f"Setting proxy {proxy}",
                     extra={'task': self.task,
                            **self.scraper.log_extras()})
        if isinstance(proxy, dict) and 'http' in proxy and 'https' in proxy:
            # Nothing more to do
            return proxy

        return {'http': proxy,
                'https': proxy
                }

    def _init_headers(self, headers):
        """Set headers for all requests

        If no user-agent is passed in, one will be provided.
        This is the only header that this will set by default

        Args:
            headers (dict): Dict of headers to set for all requests
        """
        # Set headers from init, then update with task headers
        self.session.headers = {} if headers is None else headers
        self.session.headers.update(self.task.get('headers', {}))
        # Set a UA if the scraper did not set one
        if 'user-agent' not in map(str.lower, self.session.headers.keys()):
            self._set_session_ua()

    def _init_proxy(self, proxy):
        """Set the default proxy for all requests.
        Only sets a proxy if a proxy file is set or a custom `self.get_proxy()` is set

        Args:
            proxy (str): Full url of proxy
        """
        proxy_str = proxy
        if self.task.get('proxy') is not None:
            proxy_str = self.task.get('proxy')
        # If no proxy has been passed in, try and set one
        if not proxy_str:
            proxy_str = self._get_proxy(country=self.task.get('proxy_country'))
        self.session.proxies = self._format_proxy(proxy_str)

    def _init_http_methods(self):
        # Create http methods
        self.request_get = self._set_http_method('GET')
        self.request_post = self._set_http_method('POST')
        # Not sure if these are needed, but it doesn't hurt to have them
        self.request_head = self._set_http_method('HEAD')
        self.request_put = self._set_http_method('PUT')
        self.request_patch = self._set_http_method('PATCH')
        self.request_delete = self._set_http_method('DELETE')

    def _set_http_method(self, http_method):
        def make_request(url, max_tries=3, _try_count=1, custom_source_checks=(), **r_kwargs):
            """Makes the requests to get the source file

            Must be accessed using::
                self.request_get()
                self.request_post()
                self.request_head()
                self.request_put()
                self.request_patch()
                self.request_delete()

            Args:
                max_tries (int, optional): Max times to try to get a source file.
                    Each time `self.new_profile` will be called which will
                    try and get a new proxy and new user-agent. Defaults to 3.
                _try_count (int, optional): Used to keep track of current number of tries.
                    Defaults to 1.
                custom_source_checks (list, optional): List of tuples, each inner tuple has 3 parts
                    `(regex, http_status_code, message)`
                    regex (str): A regex to try and match something in the source file
                    http status code (int): If regex gets a match, set the request status
                        code to this value
                    message (str): Custom status message to set to know this is not a normal
                        status code being thrown
                    Defaults to ().
                **r_kwargs: Keyword Arguments to be passed to requests.Session().requests

            Raises:
                ValueError: If max_tries is 0 or negative.
                HTTPIgnoreCodeError: If an ignore_code is found
                DownloadValueError: If the download failed for any reason and
                    max_tries was reached

            Returns:
                object: requests library object
            """
            if max_tries < 1:
                # TODO: Find a better error to raise
                raise ValueError("max_tries must be >= 1")

            proxy_used = self.session.proxies.get('http')
            if 'proxy' in r_kwargs:
                # Proxy is not a valid arg to pass in, so fix it
                r_kwargs['proxies'] = self._format_proxy(r_kwargs['proxy'])
                proxy_used = r_kwargs['proxies'].get('http')
                del r_kwargs['proxy']
            elif 'proxies' in r_kwargs:
                # Make sure they are in the correct format
                r_kwargs['proxies'] = self._format_proxy(r_kwargs['proxies'])
                proxy_used = r_kwargs['proxies'].get('http')

            time_of_request = datetime.datetime.utcnow().isoformat() + 'Z'
            try:
                r = self.session.request(http_method, url, **r_kwargs)

                if custom_source_checks:
                    for re_text, status_code, message in custom_source_checks:
                        if re.search(re_text, r.text):
                            r.status_code = status_code
                            r.reason = message

                log_extra = {'url': r.url,
                             'method': http_method,
                             'status_code': r.status_code,
                             'reason': r.reason,
                             'headers': {'request': dict(r.request.headers),
                                         'response': dict(r.headers)},
                             'response_time': r.elapsed.total_seconds(),
                             'time_of_request': time_of_request,
                             'num_tries': _try_count,
                             'max_tries': max_tries,
                             'task': self.task,
                             **self.scraper.log_extras(),
                             'proxy': proxy_used}
                logger.info("Request finished", extra=log_extra)

                if r.status_code != requests.codes.ok:
                    if (_try_count < max_tries
                       and r.status_code not in self._ignore_codes):
                        r_kwargs = self.new_profile(failed_response=r, **r_kwargs)
                        request_method = self._set_http_method(http_method)
                        return request_method(url,
                                              max_tries=max_tries,
                                              _try_count=_try_count + 1,
                                              custom_source_checks=custom_source_checks,
                                              **r_kwargs)
                    else:
                        if r.status_code in self._ignore_codes:
                            raise HTTPIgnoreCodeError(f"Got Ignore Code {r.status_code}",
                                                      response=r)
                        else:
                            # Log here so we can log `log_extra` data
                            logger.error("Download failed", extra=log_extra)
                            r.raise_for_status()

            except (requests.exceptions.HTTPError, HTTPIgnoreCodeError):
                raise

            except Exception as e:
                if _try_count < max_tries:
                    r_kwargs = self.new_profile(failed_response=e.response, **r_kwargs)
                    request_method = self._set_http_method(http_method)
                    return request_method(url,
                                          max_tries=max_tries,
                                          _try_count=_try_count + 1,
                                          **r_kwargs)
                else:
                    logger.exception(f"Download failed: {str(e)}",
                                     extra={'url': url,
                                            'session_headers': self.session.headers,
                                            'request_kwargs': r_kwargs,
                                            'num_tries': _try_count,
                                            'max_tries': max_tries,
                                            'task': self.task,
                                            **self.scraper.log_extras(),
                                            'proxy': proxy_used})
                    raise DownloadValueError(f"Download failed: {str(e)}")

            return r

        return make_request

    def _set_session_ua(self):
        """Set a user-agent for the request session to use
        If no `device_type` was set in the task, `desktop` will be used by default
        """
        device_type = self.task.get('device_type', 'desktop')
        ua = self._get_user_agent(device_type)
        self.session.headers.update({'user-agent': ua})

    def new_profile(self, failed_response=None, **r_kwargs):
        """Rotate proxies and headers to retry the request again
        Users scraper can override this to rotate things their own way

        Args:
            **r_kwargs: Keyword arguments passed into the request.
                Will be updated and returned back

        Returns:
            dict: Dict to be passed as keyword arguments to requests.Session().requests
        """
        # Set new UA
        # TODO: make this for headers in general
        #       this sdk will only update the UA
        #       let the scraper update more
        self._set_session_ua()

        # Set new proxy
        r_kwargs['proxy'] = self._get_proxy(country=self.task.get('proxy_country'))

        return r_kwargs

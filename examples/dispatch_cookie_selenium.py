import time
import random
import logging
import threading
import urllib.parse
from scraperx import Scraper, run_cli, Dispatch, Download, Extract

logger = logging.getLogger(__name__)


class MyDispatch(Dispatch):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.visitor_id_list = []
        self.generate_visitor_id()  # kick off the cookie generation
        self._load_tasks()

    def generate_visitor_id(self):
        """Generate a cookie 5 seconds after the last

        *Preferred way*
        If your code is BEFORE the threading.Timer, then the fn will run
        5 seconds AFTER you got the cookie. This is the safest way since you
        can never have multiple cookies being generated at once

        *Avoid if possible*
        If you code is AFTER the threading.Timer and your code takes longer
        then 10 seconds to run it will spawn a new thread even if yours is not
        complete. This can cause an issue if the threads stack up and use all
        the system resources
        """
        logger.debug("Getting new visitorId",
                     extra={'task': None,
                            'scraper_name': self.scraper.config['SCRAPER_NAME']})
        visitor_id = self._get_visitor_id()
        if visitor_id:
            self.visitor_id_list.append(visitor_id)
            logger.debug(f"Created the visitorId cookie with value {visitor_id}",
                         extra={'task': None,
                                'scraper_name': self.scraper.config['SCRAPER_NAME'],
                                'visitor_id': visitor_id})
        else:
            logger.warning("Failed to get the visitorId cookie",
                           extra={'task': None,
                                  'scraper_name': self.scraper.config['SCRAPER_NAME'],
                                  'visitor_id': visitor_id})

        # Run this fn again in 5.0 seconds
        t = threading.Timer(5.0, self.generate_visitor_id)
        t.daemon = True  # This will kill the thread if the program stops
        t.start()

    def _get_visitor_id(self):
        """Use selenium to create a session and get its cookies

        Import all selenium parts here so if not running locally
        selenium is not loaded when the downloading or extracting classe's
        are running
        """
        from selenium import webdriver
        from selenium.webdriver.firefox.options import Options as FirefoxOptions
        from webdriverdownloader import GeckoDriverDownloader

        logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
        logging.getLogger('webdriverdownloader').setLevel(logging.WARNING)
        logging.getLogger('selenium').setLevel(logging.WARNING)

        gdd = GeckoDriverDownloader()
        geckodriver = gdd.download_and_install()

        options = FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options,
                                   executable_path=geckodriver[1])

        driver.get("https://www.target.com/")
        time.sleep(2)  # wait for the page to load a bit

        # Get the correct cookie
        visitor_id = None
        for cookie in driver.get_cookies():
            if cookie['name'] == 'visitorId':
                visitor_id = cookie['value']
                break

        driver.quit()

        return visitor_id

    def _load_tasks(self):
        self.keywords = ['cookies', 'chips', 'candy']
        # When yield'ing in submit_tasks(), must set num_tasks so they
        # can be dispatched at the correct rate
        self.num_tasks = len(self.keywords)

    def submit_tasks(self):
        for idx, keyword in enumerate(self.keywords, start=1):
            yield {'keyword': keyword,
                   'store_id': '3249',
                   'ref_id': idx,
                   'device_type': 'desktop',
                   # Use a random id from the latest 5 generated
                   'visitor_id': random.choice(self.visitor_id_list[-5:]),
                   }


class MyDownload(Download):

    def download(self):
        r = self.request_get(self.gen_url())

        self.save_request(r)

    def gen_url(self):
        base_url = 'https://redsky.target.com/v2/plp/search/'
        url_prams = {'offset': 0,
                     'count': 24,
                     'keyword': self.task['keyword'],
                     'isDLP': 'false',
                     'default_purchasability_filter': 'true',
                     'include_sponsored_search': 'true',
                     'ppatok': 'AOxT33a',
                     'platform': self.task['device_type'],
                     'pageId': f"/s/{self.task['keyword']}",
                     'channel': 'web',
                     'visitorId': self.task['visitor_id'],
                     'pricing_store_id': self.task['store_id'],
                     'useragent': self.session.headers.get('user-agent'),
                     'store_ids': self.task['store_id'],
                     'key': 'eb2551e4accc14f38cc42d32fbc2b2ea',
        }
        encoded_prams = urllib.parse.urlencode(url_prams)
        return f'{base_url}?{encoded_prams}'


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        import json

        yield self.extract_task(
            name='products',
            raw_source=json.loads(raw_source)['search_response']['items']['Item'],
            idx_offset=1,
            callback=self.extract_product,
            post_extract=self.save_as,
            post_extract_kwargs={'file_format': 'json'},
        )

    def extract_product(self, item, idx, **kwargs):
        return {'title': item['title'],
                'rank': idx}


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

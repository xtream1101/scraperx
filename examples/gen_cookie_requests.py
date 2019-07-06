import urllib.parse
from scraperx import Scraper, run_cli, Dispatch, Download, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
        return {'keyword': 'cookies',
                'store_id': '3249',
                'device_type': 'desktop',
                }


class MyDownload(Download):

    def download(self):
        url = self.gen_url()
        r = self.request_get(url)

        self.save_request(r)

    def get_visitor_id(self):
        # First make a request to generate the cookie `visitorId` to be used in the finial url
        cookie_r = self.request_get('https://www.target.com/')
        return cookie_r.cookies.get_dict()['visitorId']

    def gen_url(self):
        base_url = 'https://redsky.target.com/v2/plp/search/'
        url_prams = {'offset': 0,
                     'count': 24,
                     'keyword': self.task['keyword'],
                     'isDLP': 'false',
                     'default_purchasability_filter': 'true',
                     'include_sponsored_search': 'true',
                     'ppatok': 'AOxT33a',  # seems to alwys be the same for target
                     'platform': self.task['device_type'],
                     'pageId': f"/s/{self.task['keyword']}",
                     'channel': 'web',
                     'visitorId': self.get_visitor_id(),
                     'pricing_store_id': self.task['store_id'],
                     # Use the same UA as the request
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
            callback=self.extract_products,
            post_extract=self.save_as,
            post_extract_kwargs={'file_format': 'json'},
        )

    def extract_products(self, item, idx, **kwargs):
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

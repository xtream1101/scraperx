import urllib.parse
from scraperx.write import Write
from scraperx import run_cli, BaseDispatch, BaseDownload, BaseExtract



class Dispatch(BaseDispatch):

    def submit_tasks(self):
        return {'keyword': 'cookies',
                'store_id': '3249',
                'device_type': 'desktop',
                }


class Download(BaseDownload):

    def download(self):
        url = self.gen_url()
        r = self.request_get(url)

        return Write(r.json()).write_json().save(self)

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
                     # Use the same UA as the request is sending in case that is checked to see if its the same
                     'useragent': self.session.headers.get('user-agent'),
                     'store_ids': self.task['store_id'],  # could be a list, but seems to work fine with just the one
                     'key': 'eb2551e4accc14f38cc42d32fbc2b2ea',  # seems to alwys be the same for target
        }
        encoded_prams = urllib.parse.urlencode(url_prams)
        return f'{base_url}?{encoded_prams}'


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        import json

        return {'name': 'products',
                'raw_source': json.loads(raw_source)['search_response']['items']['Item'],
                'idx_offset': 1,
                'callback': self.extract_products,
                'post_extract': self.save_as,
                'post_extract_kwargs': {'file_format': 'json',
                                        },
                }

    def extract_products(self, item, idx, **kwargs):
        return {'title': item['title'],
                'rank': idx}


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s')

    run_cli(Dispatch, Download, Extract)

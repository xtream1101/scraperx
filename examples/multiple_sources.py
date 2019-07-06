import json
from scraperx import Scraper, run_cli, Dispatch, Download, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
        keyword = 'cookies'
        return {'url': f'https://www.kroger.com/search/api/searchAll?start=0&count=24&query={keyword}&tab=0&monet=true',  # noqa: E501
                'headers': {'origin': 'https://www.kroger.com',
                            'accept-language': 'en-US,en;q=0.9',
                            'content-type': 'application/json;charset=UTF-8',
                            'accept': 'application/json, text/plain, */*',
                            'authority': 'www.kroger.com',
                            'accept-encoding': 'gzip, deflate, br',
                            'sec_req_type': 'ajax',
                            # Store: https://www.kroger.com/stores/details/035/00511
                            'division-id': '035',
                            'store-id': '00511',
                            },
                }


class MyDownload(Download):

    def download(self):
        # This first request will get the products in the search
        r1 = self.request_post(self.task['url'])

        # Saving this file will help with debugging
        self.save_request(r1, template_values={'source_name': 'upc_list'})

        # This is the data kroger needs to be sent
        data = {'upcs': r1.json()['upcs'],
                'filterBadProducts': True,
                'clicklistProductsOnly': False,
                }

        # This second request get the details from the products in the first request
        details_url = 'https://www.kroger.com/products/api/products/details'
        r2 = self.request_post(details_url, json=data)
        self.save_request(r2, template_values={'source_name': 'details'})


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        if source_idx == 1:
            # Only need to extract the data of the second request
            yield self.extract_task(
                name='products',
                raw_source=json.loads(raw_source)['products'],
                callback=self.extract_product,
                post_extract=self.save_as,
                post_extract_kwargs={'file_format': 'json'},
            )

    def extract_product(self, item, idx, **kwargs):
        # Return data so it is still saved
        return {'title': item.get('description', '')}


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

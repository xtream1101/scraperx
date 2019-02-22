import json
from scraperx import run, BaseDispatch, BaseDownload, BaseExtract
from scraperx.trigger import run_task


class Dispatch(BaseDispatch):

    def create_tasks(self):
        keyword = 'cookies'
        return {'url': f'https://www.kroger.com/search/api/searchAll?start=0&count=24&query={keyword}&tab=0&monet=true',
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


class Download(BaseDownload):

    def download(self):
        # This first request will get the products in the search
        search_results = self.request_post(self.task['url'])

        # Saving this file will help with debugging
        search_results.write_file().save(self,
                                         template_values={'source_name': 'upc_list'})

        # This is the data kroger needs to be sent
        data = {'upcs': search_results.r.json()['upcs'],
                'filterBadProducts': True,
                'clicklistProductsOnly': False,
                }

        # This second request get the details from the products in the first request
        details_url = 'https://www.kroger.com/products/api/products/details'
        return self.request_post(details_url, json=data)\
                   .write_file().save(self,
                                      template_values={'source_name': 'details'})


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        return {'name': 'products',
                'raw_source': json.loads(raw_source)['products'],
                'callback': self.extract_product,
                'save_as': 'json',
                }

    def extract_product(self, item, idx, **kwargs):

        # Trigger the downlaod
        # Return data so it is still saved
        return {'title': item.get('description', '')}


if __name__ == '__main__':
    run(Dispatch, Download, Extract)

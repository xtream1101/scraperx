from scraperx.write import Write
from scraperx import run_cli, BaseDispatch, BaseDownload, BaseExtract


class Dispatch(BaseDispatch):

    def submit_tasks(self):
        return {'url': 'http://testing-ground.scraping.pro/blocks'}


class Download(BaseDownload):

    def download(self):
        r = self.request_get(self.task['url'])
        self.save_request(r)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        return {'name': 'products',
                'selectors': ['#case1 > div:not(.ads)'],
                'callback': self.extract_product,
                'post_extract': self.save_as,  # TODO: add docs about what builtin's there are
                'post_extract_kwargs': {'file_format': 'json',
                                        },
                # TODO: Link to docs regarding what qa can do
                'qa': {
                    'title': {
                        'required': True,
                        'max_length': 128,
                    },
                    'price': {
                        'type': float,
                    }
                }
        }

    def extract_product(self, element, idx, **kwargs):
        return {'title': element.css('div.name').xpath('string()').extract_first(),
                'price': float(element.css('span::text').extract()[1].replace('$', '').replace(',', '')),
                }


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s')

    run_cli(Dispatch, Download, Extract)

from scraperx import run_cli, BaseDispatch, BaseDownload, BaseExtract
from scraperx.write import Write


class Dispatch(BaseDispatch):

    def create_tasks(self):
        return {'url': 'http://testing-ground.scraping.pro/blocks'}


class Download(BaseDownload):

    def download(self):
        r = self.request_get(self.task['url'])

        return Write(r.text).write_file().save(self)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        return [{'name': 'case1_products',
                 'selectors': ['#case1 > div:not(.ads)'],
                 'callback': self.extract_product,
                 'post_extract': self.save_as,
                 'post_extract_kwargs': {'file_format': 'json',
                                         'template_values': {'extractor_name': 'case1_products'},
                                         },
                 },
                {'name': 'case2_products',
                 'selectors': ['#case2 > div.left > div:not(.ads)'],
                 'callback': self.extract_product,
                 'post_extract': self.save_as,
                 'post_extract_kwargs': {'file_format': 'json',
                                         'template_values': {'extractor_name': 'case2_products'},
                                         },
                 },
                ]

    def extract_product(self, element, idx, **kwargs):
        return {'title': element.css('div.name').xpath('string()').extract_first()}


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s')

    run_cli(Dispatch, Download, Extract)

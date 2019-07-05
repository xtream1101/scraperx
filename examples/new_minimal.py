from scraperx import Scraper, run_cli, Dispatch, Download, Extract


class MinimalDispatch(Dispatch):
    def submit_tasks(self):
        return {'url': 'http://testing-ground.scraping.pro/blocks'}


class MinimalExtract(Extract):

    def extract(self, raw_source, source_idx):

        return {'name': 'products',
                'selectors': ['#case1 > div:not(.ads)'],
                'callback': self.extract_product,
                'post_extract': self.save_as,  # TODO: add docs about what builtin's there are
                'post_extract_kwargs': {'file_format': 'json',
                                        },
                }

    def extract_product(self, element, idx, **kwargs):
        return {'title': element.css('div.name').xpath('string()').extract_first()}


my_scraper = Scraper(dispatch_cls=MinimalDispatch, extract_cls=MinimalExtract)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    run_cli(my_scraper)

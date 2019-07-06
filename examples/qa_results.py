from scraperx import Scraper, run_cli, Dispatch, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
        return {'url': 'http://testing-ground.scraping.pro/blocks'}


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        yield self.extract_task(
            name='products',
            selectors=['#case1 > div:not(.ads)'],
            callback=self.extract_product,
            post_extract=self.save_as,
            post_extract_kwargs={'file_format': 'json'},
            qa={
                'title': {
                    'required': True,
                    'max_length': 128,
                },
                'price': {
                    'type': float,
                }
            }
        )

    def extract_product(self, element, idx, **kwargs):
        return {'title': element.css('div.name').xpath('string()').extract_first(),
                'price': float(element.css('span::text').extract()[1]
                                      .replace('$', '').replace(',', '')),
                }


my_scraper = Scraper(dispatch_cls=MyDispatch,
                     extract_cls=MyExtract)

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s'
    )
    run_cli(my_scraper)

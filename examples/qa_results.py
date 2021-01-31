from scraperx import Scraper, run_cli, Dispatch, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
        return {'url': 'https://www.imdb.com/chart/top/'}


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        yield self.extract_task(
            name='products',
            selectors=['tbody.lister-list tr'],
            callback=self.extract_product,
            post_extract=self.save_as,
            post_extract_kwargs={'file_format': 'json'},
            qa={
                'title': {'required': True, 'max_length': 128},
                'year': {'type': int, 'required': True},
                'rating': {'type': float},
            }
        )

    def extract_product(self, element, idx, **kwargs):
        return {
            'title': element.css('td.titleColumn a').xpath('string()').extract_first(),
            'year': int(element.css('span.secondaryInfo').xpath('string()').extract_first()[1:-1]),
            'rating': float(element.css('td.ratingColumn ').xpath('string()').extract_first()),
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

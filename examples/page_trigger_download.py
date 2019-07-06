from scraperx import Scraper, run_cli, run_task, Dispatch, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
        return {'url': 'https://www.webscraper.io/test-sites/e-commerce/static/computers/laptops',
                'page': 1,  # Always starts on page 1
                }


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        yield self.extract_task(
            name='products',
            selectors=['h1 + div.row > div'],
            idx_offset=1,
            callback=self.extract_products,
            post_extract=self.save_as,
            post_extract_kwargs={'file_format': 'json'},
        )
        yield self.extract_task(
            name='page',
            selectors=['h1 ~ ul.pagination'],
            callback=self.extract_next_page
        )

    def extract_products(self, element, idx, **kwargs):
        return {'title': element.css('div.caption a').xpath('string()').extract_first(),
                'rank': idx,
                'page': self.task['page'],
                }

    def extract_next_page(self, element, idx, **kwargs):
        # No need to save any data, just here to trigger the next task
        if self.task['page'] < 5:
            # Only get the first 5 pages
            # Normally if the number of pages is known, you would add each page to the dispatcher.
            # This is just to demo if you have an unknown amount of pages on how to trigger the nex.
            next_page = element.css('a[rel="next"]').xpath('@href').extract_first()
            new_task = {'url': next_page,
                        'page': self.task['page'] + 1,
                        }
            run_task(self.scraper, new_task, task_cls=self.scraper.download)


my_scraper = Scraper(dispatch_cls=MyDispatch,
                     extract_cls=MyExtract)

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s'
    )
    run_cli(my_scraper)

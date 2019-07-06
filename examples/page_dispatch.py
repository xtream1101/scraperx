from parsel import Selector
from scraperx import Scraper, run_cli, Dispatch, Download, Extract


class MyDispatch(Dispatch):
    base_url = 'http://webscraperio.us-east-1.elasticbeanstalk.com/test-sites/e-commerce/static/computers/tablets'  # noqa: E501

    def submit_tasks(self):
        max_page = self._get_max_page()
        tasks = []
        for page in range(1, max_page + 1):
            tasks.append({'url': f'{self.base_url}?page={page}',
                          'page': page,
                          })
        return tasks

    def _get_max_page(self):
        task = {'url': self.base_url}
        source_data = DispatchDownloadHelper(self.scraper, task).download()
        element = Selector(text=source_data)
        max_page = element.css('h1 ~ ul.pagination li').xpath('string()').extract()[-2]
        return int(max_page)


class DispatchDownloadHelper(Download):

    def download(self):
        return self.request_get(self.task['url']).text


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

    def extract_products(self, element, idx, **kwargs):
        return {'title': element.css('div.caption a').xpath('string()').extract_first(),
                'rank': idx,
                'page': self.task['page'],
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

from scraperx import run, BaseDispatch, BaseDownload, BaseExtract
from parsel import Selector


class Dispatch(BaseDispatch):
    base_url = 'http://webscraperio.us-east-1.elasticbeanstalk.com/test-sites/e-commerce/static/computers/tablets'

    def create_tasks(self):
        max_page = self._get_max_page()
        tasks = []
        for page in range(1, max_page + 1):
            tasks.append({'url': f'{self.base_url}?page={page}',
                          'page': page,
                          })
        return tasks

    def _get_max_page(self):
        task = {'url': self.base_url}
        source_data = DispatchDownloadHelper(task).download()
        element = Selector(text=source_data)
        max_page = element.css('h1 ~ ul.pagination li').xpath('string()').extract()[-2]
        return int(max_page)


class DispatchDownloadHelper(BaseDownload):

    def download(self):
        return self.request_get(self.task['url']).source


class Download(BaseDownload):

    def download(self):
        return self.request_get(self.task['url']).write_file().save(self)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        return {'name': 'products',
                'selectors': ['h1 + div.row > div'],
                'idx_offset': 1,
                'callback': self.extract_products,
                'save_as': 'json',
                }

    def extract_products(self, element, idx, **kwargs):
        return {'title': element.css('div.caption a').xpath('string()').extract_first(),
                'rank': idx,
                'page': self.task['page'],
                }


if __name__ == '__main__':
    run(Dispatch, Download, Extract)

from scraperx import run, BaseDispatch, BaseDownload, BaseExtract
from scraperx.trigger import run_task


class Dispatch(BaseDispatch):

    def create_tasks(self):
        return {'url': 'https://www.webscraper.io/test-sites/e-commerce/static/computers/laptops',
                'page': 1,  # Always starts on page 1
                }


class Download(BaseDownload):

    def download(self):
        return self.request_get(self.task['url']).write_file().save(self)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        return [{'name': 'products',
                 'selectors': ['h1 + div.row > div'],
                 'idx_offset': 1,
                 'callback': self.extract_products,
                 'save_as': 'json',
                 },
                {'name': 'page',
                 'selectors': ['h1 ~ ul.pagination'],
                 'callback': self.extract_next_page,
                 }
                ]

    def extract_products(self, element, idx, **kwargs):
        return {'title': element.css('div.caption a').xpath('string()').extract_first(),
                'rank': idx,
                'page': self.task['page'],
                }

    def extract_next_page(self, element, idx, **kwargs):
        # No need to save any data, just here to trigger the next task
        if self.task['page'] < 5:
            # Only get the first 5 pages
            next_page = element.css('a[rel="next"]').xpath('@href').extract_first()
            new_task = {'url': next_page,
                        'page': self.task['page'] + 1,
                        }
            run_task(new_task, Download)


if __name__ == '__main__':
    run(Dispatch, Download, Extract)

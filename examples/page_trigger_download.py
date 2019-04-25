from scraperx.write import Write
from scraperx.trigger import run_task
from scraperx import run_cli, BaseDispatch, BaseDownload, BaseExtract


class Dispatch(BaseDispatch):

    def submit_tasks(self):
        return {'url': 'https://www.webscraper.io/test-sites/e-commerce/static/computers/laptops',
                'page': 1,  # Always starts on page 1
                }


class Download(BaseDownload):

    def download(self):
        r = self.request_get(self.task['url'])
        return Write(r.text).write_file().save(self)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        return [{'name': 'products',
                 'selectors': ['h1 + div.row > div'],
                 'idx_offset': 1,
                 'callback': self.extract_products,
                 'post_extract': self.save_as,
                 'post_extract_kwargs': {'file_format': 'json',
                                         },
                 },
                {'name': 'page',
                 'selectors': ['h1 ~ ul.pagination'],
                 'callback': self.extract_next_page,
                 # No need to save any data, just here to trigger the next task
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
            # Normally if the number of pages is known, you would add each page to the dispatcher.
            # This is just to demo if you have an unknown amount of pages on how to trigger the next.
            next_page = element.css('a[rel="next"]').xpath('@href').extract_first()
            new_task = {'url': next_page,
                        'page': self.task['page'] + 1,
                        }
            run_task(new_task, task_cls=Download, extract_cls=Extract)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s')

    run_cli(Dispatch, Download, Extract)

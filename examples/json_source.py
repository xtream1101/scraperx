from scraperx import run_cli, BaseDispatch, BaseDownload, BaseExtract


class Dispatch(BaseDispatch):

    def submit_tasks(self):
        return {'url': 'https://www.reddit.com/r/funny.json'}


class Download(BaseDownload):

    def download(self):
        r = self.request_get(self.task['url'])

        self.save_request(r)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        import json

        return {'name': 'products',
                'raw_source': json.loads(raw_source)['data']['children'],
                'callback': self.extract_posts,
                'post_extract': self.save_as,
                'post_extract_kwargs': {'file_format': 'json',
                                        },
                }

    def extract_posts(self, item, idx, **kwargs):
        return {'title': item['data'].get('title')}


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s')

    run_cli(Dispatch, Download, Extract)

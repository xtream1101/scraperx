from scraperx import run, BaseDispatch, BaseDownload, BaseExtract


class Dispatch(BaseDispatch):

    def create_tasks(self):
        return {'url': 'https://www.reddit.com/r/funny.json'}


class Download(BaseDownload):

    def download(self):
        return self.request_get(self.task['url']).write_json().save(self)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        import json

        return {'name': 'products',
                'raw_source': json.loads(raw_source)['data']['children'],
                'callback': self.extract_posts,
                'save_as': 'json',
                }

    def extract_posts(self, item, idx, **kwargs):
        return {'title': item['data'].get('title')}


if __name__ == '__main__':
    run(Dispatch, Download, Extract)

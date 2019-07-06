from scraperx import Scraper, run_cli, Dispatch, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
        return {'url': 'https://www.reddit.com/r/funny.json'}


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        import json

        yield self.extract_task(
            name='products',
            raw_source=json.loads(raw_source)['data']['children'],
            callback=self.extract_posts,
            post_extract=self.save_as,
            post_extract_kwargs={'file_format': 'json'},
        )

    def extract_posts(self, item, idx, **kwargs):
        return {'title': item['data'].get('title')}


my_scraper = Scraper(dispatch_cls=MyDispatch,
                     extract_cls=MyExtract)

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s'
    )
    run_cli(my_scraper)

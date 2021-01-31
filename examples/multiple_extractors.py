from scraperx import Scraper, run_cli, Dispatch, Extract


class MyDispatch(Dispatch):

    def submit_tasks(self):
        return {'url': 'https://www.imdb.com/search/title/?title_type=feature&num_votes=10000,&genres=history&languages=en&sort=user_rating,desc&explore=genres&view=simple'}  # noqa: E501


class MyExtract(Extract):

    def extract(self, raw_source, source_idx):
        yield self.extract_task(
            name='movies',
            selectors=['div.lister-item'],
            callback=self.extract_movie,
            post_extract=self.save_as,
            post_extract_kwargs={
                'file_format': 'json',
                'template_values': {'extractor_name': 'movies'},
            },
        )
        yield self.extract_task(
            name='genres',
            selectors=['div.aux-content-widget-2 td'],
            callback=self.extract_genre,
            post_extract=self.save_as,
            post_extract_kwargs={
                'file_format': 'json',
                'template_values': {'extractor_name': 'genres'},
            },
        )

    def extract_movie(self, element, idx, **kwargs):
        return {'title': element.css('span[title] a').xpath('string()').extract_first()}

    def extract_genre(self, element, idx, **kwargs):
        return {'name': element.css('a').xpath('string()').extract_first()}


my_scraper = Scraper(dispatch_cls=MyDispatch,
                     extract_cls=MyExtract)

if __name__ == '__main__':
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - [%(scraper_name)s] %(message)s'
    )
    run_cli(my_scraper)

from scraperx import run, BaseDispatch, BaseDownload, BaseExtract


class Dispatch(BaseDispatch):

    def create_tasks(self):
        return {'url': 'http://testing-ground.scraping.pro/blocks'}


class Download(BaseDownload):

    def download(self):
        return self.request_get(self.task['url']).write_file().save(self)


class Extract(BaseExtract):

    def extract(self, raw_source, source_idx):
        return {
            'name': 'products',
            'selectors': ['#case1 > div:not(.ads)'],
            'callback': self.extract_product,
            'save_as': 'json',
            # TODO: Link to docs regarding what qa can do
            'qa': {
                'title': {
                    'required': True,
                    'max_length': 128,
                },
                'price': {
                    'type': float,
                }
            }
        }

    def extract_product(self, element, idx, **kwargs):
        return {'title': element.css('div.name').xpath('string()').extract_first(),
                'price': float(element.css('span::text').extract()[1].replace('$', '').replace(',', '')),
                }


if __name__ == '__main__':
    run(Dispatch, Download, Extract)

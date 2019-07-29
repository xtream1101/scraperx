.. ScraperX documentation master file, created by
   sphinx-quickstart on Mon Jul 22 10:02:52 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to ScraperX's documentation!
====================================

The Basics
----------

.. code-block:: python3
   :caption: my_scraper.py

    from scraperx import Scraper, run_cli, Dispatch, Extract

    class MyDispatch(Dispatch):
        def submit_tasks(self):
            return {'url': 'http://testing-ground.scraping.pro/blocks'}

    class MyExtract(Extract):
        def extract(self, raw_source, source_idx):
            yield self.extract_task(
                name='products',
                selectors=['#case1 > div:not(.ads)'],
                callback=self.extract_product,
                post_extract=self.save_as,
                post_extract_kwargs={'file_format': 'json'},
            )
        def extract_product(self, element, idx, **kwargs):
            return {'title': element.css('div.name').xpath('string()').extract_first()}

    my_scraper = Scraper(dispatch_cls=MyDispatch,
                         extract_cls=MyExtract)

    if __name__ == '__main__':
        run_cli(my_scraper)

Above is about as simple a scraper can get. This scraper is going to download and extract a single page as seen in the ``submit_tasks`` function.

By default it will download the source file and save the extracted output in the relative directory ``output/``.

Run this scraper by running ``python my_scraper.py dispatch``


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   dispatching



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

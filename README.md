# ScraperX  SDK

Docs are a WIP.  
Right now its just me (eddy) jotting down some notes, will re-format and clean up later.


## Dispatching

### Task data
This is a dict of values that is passed to each step of the process. The scraper can put anything it wants here that it may need. But here are a few build in values that are not requiored, but are used if you do supply them:

- `headers`: Dict of headers to use each request
- `proxy`: Full proxy string to be used
- `geo_alpha2`: Used to get a proxy for this region, if this and `proxy` are not set, a random proxy will be used.
- `platform`: Used when selecting a proxy if one was not passed in. Will keep from selecting proxies that do not work on a given site.
- `device_type`: used when setting a user-agent if one was not set. Options are `desktop` or `mobile`

## Downloading

Uses a requests.Session to make get and post requests.
The __init__ of the BaseDownload class can take the following args:
- task: needed no matter what
- headers: dict to set headers for the whole session. default: random User-Agent for the device type, will use desktop if no device type is set)
- proxy: the proxy string to use for the requests


When using BaseDownloader, a requests session is created under `self.session`, so every get/post you make will use the same session per task.
you can also set headers per call to by pasing the keyword args to self.get() and self.post(). Any kwargs you pass to self.get/post will be pased to the sessions get/post methods.  

When using BaseDownloader's get & post functions, it will use the requests session created in __init__ and return you a Request object. From that, you can acess the raw requests request from `self.r` and the raw source at `self.source` (`r.text`)  

The Request object also gives full access to the WriteTo class (see <here> about what that has)  


A request will retry n (3 by default) times to get a successful status code, each retry it will try and trigger a function called `new_profile()` where you have the chance to switch the headers/proxy the request is using (will only update for that request?). If that function does not exist, it will try again with the same data.


### Setting headers/proxies

The ones set in the `self.get/post` will be combines with the ones set in the `__init__` and override if the key is the same.  

self.get/post kwargs headers/proxy
will override
self.task[headers/proxy]
will override
__init__ kwargs headers/proxy

Any header/proxy set on the request (get/post/etc) will only be set for that single request. For those values to be set in the session they must be set from the init or be in the task data.


## Extracting
Coming to a Readme near you...


## Config

3 Ways of setting config values:
- CLI Argument: Will override any other type of config value. Use `-h` to see available options
- Environment variable: Will override a config value in the yaml
- Yaml file: Will use these values if no other way is set for a key


### Config values

```yaml
# config.yaml
# This is a config file with all config values
# Required fields are marked as such

default:
  extractor:
    save_data:
      service: local  # (local, s3) Default: local
      bucket_name: my-extracted-data  # Required if service is s3, if local this is not needed
    file_template: test_output/{scraper_name}/{id}_extracted.json  # Optional, if not set then a file name must be passed in when saving

  downloader:
    save_data:
      service: local  # (local, s3) Default: local
      bucket_name: my-downloaded-data  # Required if service is s3, if local this is not needed
    file_template: test_output/{scraper_name}/{id}_source.html  # Optional, if not set then a file name must be passed in when saving

  dispatch:
    limit: 5  # Default None. Max number of tasks to dispatch. Do not set to run all tasks
    service:
      # This is where both the download and extractor services will run
      name: local  # (local, sns) Default: local
      sns_arn: sns:arn:of:service:to:trigger  # Required if type is sns, if local this is not needed
    ratelimit:
      type: qps  # (qps, period) Required. `qps`: Queries per second to dispatch the tasks at. `period`: The time in hours to dispatch all of the tasks in.
      value: 1  # Required. Can be an int or a float. When using period, value is in hours
```

If you are using the `*_file_template` config, a python `.format()` runs on this string so you can use `{key_name}` to make it dynamic. The keys you will have direct access to are the following:
  - All keys in your task that was dispatched
  - Any thing you pass into the `template_values={}` kwarg for the `.save()` fn
  - `time_downloaded`: time (utc) passed from the downloader (in both the downlaoder and extractor)
  - `date_downloaded`: date (utc) passed from the downloader (in both the downlaoder and extractor)
  - `time_extracted`: time (utc) passed from the extractor (just in the extractor)
  - `date_extracted`: date (utc) passed from the extractor (just in the extractor)

Anything under the `default` section can also have its own value per scraper. So if we have a scraper named `search` and we want it to use a different rate limit then all the other scrapers you can do:
```yaml
search:
  dispatch:
    ratelimit:
      type: period
      value: 5
```



### Sample scraper

```python
import logging
from scraperx import (run, BaseDispatch, BaseDownload,
                      BaseExtract, BaseExtractor)

logger = logging.getLogger(__name__)


class Dispatch(BaseDispatch):

    def create_tasks(self):
        """Gather the data that needs to be scraped

        Used to create a list of dicts that contain the data to send to the downloader
        """
        tasks = []
        for pg in range(1, 3):
            # Here is where you hit your db or some other source to get the data you want
            tasks.append({'url': f'https://example.com/search?page={pg}',
                          'page': pg,
                          'scraper_name': 'Test Test Test',
                          })
        return tasks


class Download(BaseDownload):

    def __init__(self, *args, **kwargs):
        # ignore_codes are status codes it will not re try on
        super().__init__(*args, **kwargs, ignore_codes=[404, 429])

    def download(self):
        # Will raise `r.raise_for_status()` If it is a failed staus code
        # No need to catch it here unless you want to do something with it
        request = self.get(self.task['url'])
        # Save the data
        file_data = request.write_file().save(self)

        # TODO: Make it so the scraper is not required to return the saved files
        return file_data


class Extract(BaseExtract):

    def extract(self, raw_source):
        """Extracts the data

        The input is the download output

        Run the code that extracts the data from the source passed in

        Arguments:
            task {dict} -- the task from the download() output
        """
        extracted_data = SomeExtractor(self.task, raw_source)
        extracted_data_file = extracted_data.write_json().save(self)

        return extracted_data_file


class SomeExtractor(BaseExtractor):

    def __init__(self, task, raw_source):
        super().__init__(task, raw_source)

        # List of css selectors that will return the items of the page you want
        result_selectors = ['#item-wrapper']
        self.extract_results(result_selectors)

    def extract_result(self, idx, element):
        """Extract the contents of a single result

        Arguments:
            idx {int} - The index of the element in the list of results
            element {Parsel object} -- Element of a single element selected by
                                       the result_selectors in the __init__

        Returns:
            dict -- The data to be saved from this element
        """
        data = {'rank': idx,
                'page': self.task['page'],
                }

        # Get the title of the product
        title_selector = '#main div.item-page-info h1::text'
        data['title'] = element.css(title_selector).extract_first().strip()

        # Return the data for a single extracted item
        return data


if __name__ == '__main__':
    run(Dispatch, Download, Extract)
```

# ScraperX  SDK

[![PyPI](https://img.shields.io/pypi/v/scraperx.svg)](https://pypi.org/project/scraperx/)  
[![PyPI](https://img.shields.io/pypi/l/scraperx.svg)](https://pypi.org/project/scraperx/)  


### Getting Started

1. Create a new directory where the scraper will live and add the following files:
    - A config file: `config.yaml` [(Example)](./examples/config.yaml)
    - The scraper file: `your_scraper.py` [(Example)](./examples/minimal.py)
1. Next install this library from pypi: `pip install scraperx`
1. Run the full scraper by running `python your_scraper.py dispatch`
    - To see the arguments for the command: `python your_scraper.py dispatch -h`
    - See all the commands available: `python your_scraper.py -h`

#### Sample scrapers
Sample scrapers can be found in the [examples](./examples) folder of this repo


## Developing

Any time the scraper needs to override the bases `__init__`, always pass in `*args` & `**kwargs` like so:  
```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
```

### Dispatching

#### Task data
This is a dict of values that is passed to each step of the process. The scraper can put anything it wants here that it may need. But here are a few build in values that are not requiored, but are used if you do supply them:

- `headers`: Dict of headers to use each request
- `proxy`: Full proxy string to be used
- `proxy_country`: Used to get a proxy for this region, if this and `proxy` are not set, a random proxy will be used.
- `device_type`: used when setting a user-agent if one was not set. Options are `desktop` or `mobile`

### Downloading

Uses a `requests.Session` to make get and post requests.
The `__init__` of the `BaseDownload` class can take the following args:
- task: Required. The task from the dispatcher
- headers: (Named arg) dict to set headers for the whole session. default: random User-Agent for the device type, will use desktop if no device type is set
- proxy: (Named arg) Proxy string to use for the requests
- ignore_codes: (Named arg) List of HTTP Status codes to not retry on. If these codes are seen, it will treat the request as any other success. 


When using BaseDownloader, a requests session is created under `self.session`, so every get/post you make will use the same session per task.
Headers can also be set per call by pasing the keyword args to `self.request_get()` and `self.request_post()`. Any kwargs you pass to self.request_get/post will be pased to the sessions get/post methods.

When using BaseDownloader's get & post functions, it will use the requests session created in __init__ and a python `requests` response object.

A request will retry _n_ times (3 by default) to get a successful status code, each retry it will try and trigger a function called `new_profile()` where you have the chance to switch the headers/proxy the request is using (will only update for that request?). If that function does not exist, it will try again with the same data.

There are a few custom arguments that can be passed into the `self.request_*` functions that this sdk will use. All others will be passed to the `requests` methods call.  
Named arguments:
- **max_tries**: Default=3. Type int. The number of tries a request will be tried, each try it will try and get a new proxy and User-Agent
- **custom_source_checks**: Default=None. Type list of lists. Used to set the request to a set status code based on a regex that runs on the page source.
    - This will look to see if the words _captcha_ are in the source page and set that response status code to a 403, with the status message being _Capacha Found_. The status message is there so you know if it is a real 403 or your custom status.
        -  `[(re.compile(r'captcha', re.I), 403, 'Capacha Found')]`

#### Saving the source
This is required for the extractor to run on the downloaded data. Inside of `self.download()` just call `self.save_request(r)` on the request that was made. This will add the source file to a list of saved sources that will be passed to the extractor for parsing.  
Some keyword arguments that can be passed into `self.save_request`  
- **template_values** _{dict}_ - Additonal keys to use in the template
- **filename** _{str}_ - Overide the filename from the template_name in the config

#### Download Exceptions
These exceptions will be raised when calling `self.request_*`. They will be caught saftly so the scraper does not need to catch them. But if the scraper wanted to do something based on the exception, there can be a `try/except` around the scrapers `self.request_*`.  

 - `scraperx.exceptions.DownloadValueError`: If there is an exception that is not caught by the others
 - `scraperx.exceptions.HTTPIgnoreCodeError`: When the status code of the request is found in the `ignore_codes` argument of BaseDownload
 - `requests.exceptions.HTTPError`: When the requests returns a non successful status code and was not found in `ignore_codes` 

#### Setting headers/proxies

The ones set in the `self.request_get/request_post` will be combined with the ones set in the `__init__` and override if the key is the same.

self.request_get/request_post kwargs headers/proxy  
will override  
self.task[headers/proxy]  
will override  
__init__ kwargs headers/proxy  

Any header/proxy set on the request (get/post/etc) will only be set for that single request. For those values to be set in the session they must be set from the init or be in the task data.

#### Proxies
If you have a list of proxies that the downloader should auto rotate between they can be saved in a csv in the following format:
```csv
proxy,country
http://user:pass@some-server.com:5500,US
http://user:pass@some-server.com:5501,US
http://user:pass@some-server.com:6501,DE
```
Set the env var `PROXY_FILE` to the path of the above csv for the scraper to load it in.  
If you have not passed in a proxy directly in the task and this proxy csv exists, then it will pull a random proxy from this file. It will use the `proxy_country` if set in the task data to select the correct country to proxy to.

#### User-Agent
If you have not directly set a user-agent, a random one will be pulled based on the `device_type` in the task data.  
If `device_type` is not set, it will default to use a desktop user-agent.
To set your own list of user-agents to choose from, create a csv in the following format:  
```csv
device_type,user_agent
desktop,"Some User Agent for desktop"
desktop,"Another User Agent for desktop"
mobile,"Now one for mobile"
```
Set the env var `UA_FILE` to the path of the above csv for the scraper to load it in.  


### Extracting

[Parsel documentation](https://parsel.readthedocs.io/en/latest/)  

#### Data extraction helpers
`find_css_elements(source, css_selectors)`  
  - `source` - Parsel object to run the css selectors on
  - `css_selectors` - A list of css selectors to try and extract the data

Returns a Parsel element from the first css selector that returns data.  

This snippet would be in the scrapers `Extract(BaseExtract)` class, used in the method that is extracting the data.
```python
title_selectors = ['h3',
                   'span.title',
                   ]
result['title'] = self.find_css_elements(element, title_selectors)\
                      .xpath('string()').extract_first().strip()
```

There are a few built in parsers that can assist with extracting some types of data 
```python
from scraperx import parsers

###
# Price
###
# This will parse the price out of a string and return the low and high values as floats
raw_p1_str = '15,48€'
p1 = parsers.price(raw_p1_str)
# p1 = {'low': 15.48, 'high': None}

raw_p2_str = '1,999'
p2 = parsers.price(raw_p2_str)
# p2 = {'low': 1999.0, 'high': None}

raw_p3_str = '$49.95 - $99.99'
p3 = parsers.price(raw_p3_str)
# p3 = {'low': 49.95, 'high': 99.99}


###
# Rating
###
# Parse the rating from astring
# Examples: https://regex101.com/r/ChmgmF/3
raw_r1_str = '4.4 out of 5 stars'
r1 = parsers.rating(raw_r1_str)
# r1 = 4.4

raw_r2_str = 'An average of 4.1 star'
r2 = parsers.rating(raw_r2_str)
# r2 = 4.1
```

If there are more cases you would like these parsers to catch please open up an issue with the use case you are trying to parse.


### Testing
When updating the extractors there is a chance that it will not work with the previous source files. So having a source and its QA'd data file is useful to test against to verify that data is still extracting correctly.

#### Creating test files
1. Run `python your_scraper.py create-test path_to/metadata_source_file`
    - The input file is the `*_metadata.json` file that gets created when you run the scraper and it downloads the source files.
2. This will copy the metadata file and the sources into the directory `tests/sample_data/your_scraper/` using the time the source was downloaded (from the metadata) as the file name.
    - It also creates extracted qa files for each of the sources based on your extractors.
    - it extracts the data in json format to make it easy to qa and read.
3. The QA files it created will have `_extracted_(qa)_` in the file name. What you have to do it confirm that all values are correct in that file. If everything looks good then fix the file name from having `_extracted_(qa)_` to `_extracted_qa_`. This will let the system know that the file has been checked and that is the data it will use to compare when testing.
4. Create an empty file `tests/__init__.py`. This is needed for the tests to run.
5. Next is to create the code that will run the tests. Create the file `tests/tests.py` with the contents below
```python
import unittest  # The testingframe work to use
from scraperx.test import ExtractorBaseTest  # Does all the heavy lifting for the test
from your_scraper import Extract as YourScraperExtract  # Your scrapers extract class
# If you have multiple scrapers, then import their extract classes here as well


# This test will loop through all the test files for the scraper
class YourScraper(ExtractorBaseTest.TestCase):

    def __init__(self, *args, **kwargs):
        # The directory that the test files for your scraper are in
        data_dir = 'tests/sample_data/your_scraper'
        # ignore_keys will not test the qa values to the current extracted test value. This is most useful when dealing with timestamps or other values that will change on each time the data is extracted
        super().__init__(data_dir, YourScraperExtract, ignore_keys=['time_extracted'], *args, **kwargs)

# If you have multiple scrapers, then create a class for each

# Feel free to include any other unit tests you may want to run as well
```
6. Running the tests `python -m unittest discover -vv`


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
      bucket_name: my-extracted-data  # Required if `service` is s3, if local this is not needed
    file_template: test_output/{scraper_name}/{id}_extracted.json  # Optional, Default is "output/source.html"

  downloader:
    save_data:
      service: local  # (local, s3) Default: local
      bucket_name: my-downloaded-data  # Required if `service` is s3, if local this is not needed
    file_template: test_output/{scraper_name}/{id}_source.html  # Optional, Default is "output/extracted.json"

  dispatch:
    limit: 5  # Default None. Max number of tasks to dispatch. If not set, all tasks will run
    service:
      # This is where both the download and extractor services will run
      name: local  # (local, sns) Default: local
      sns_arn: sns:arn:of:service:to:trigger  # Required if `name` is sns, if local this is not needed
    ratelimit:
      type: qps  # (qps, period) Required. `qps`: Queries per second to dispatch the tasks at. `period`: The time in hours to dispatch all of the tasks in.
      value: 1  # Required. Can be an int or a float. When using period, value is in hours
```

If you are using the `file_template` config, a python `.format()` runs on this string so you can use `{key_name}` to make it dynamic. The keys you will have direct access to are the following:
  - All keys in your task that was dispatched
  - Any thing you pass into the `template_values={}` kwarg for the `.save()` fn
  - `time_downloaded`: time (utc) passed from the downloader (in both the downlaoder and extractor)
  - `date_downloaded`: date (utc) passed from the downloader (in both the downlaoder and extractor)
  - `time_extracted`: time (utc) passed from the extractor (just in the extractor)
  - `date_extracted`: date (utc) passed from the extractor (just in the extractor)

Anything under the `default` section can also have its own value per scraper. So if we have a scraper named `search` and we want it to use a different rate limit then all the other scrapers you can do:
```yaml
# Name of the python file
search:
  dispatch:
    ratelimit:
      type: period
      value: 5
```

To override the `value` in the above snippet using an environment variable, set `DISPATCH_RATELIMIT_VALUE=1`. This will overide all dispatch ratelimit values in default and custom.


## Issues
If you run into the error `may have been in progress in another thread when fork() was called.` when running the scraper locally on a mac. Then set the env var `export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`  
This is because of a security setting on macs when spawning threads from threads https://github.com/ansible/ansible/issues/32499#issuecomment-341578864

# ScraperX  SDK

Docs are a WIP.  
Right now its just me (eddy) jotting down some notes, will re-format and clean up later.


## Dispatching

### Task data
This is a dict of values that is passed to each step of the process. The scraper can put anything it wants here that it may need. But here are a few build in values that are not requiored, but are used if you do supply them:

- `headers`: Dict of headers to use each request
- `cookies`: Dict of cookies to use in each request
- `proxy`: Full proxy string to be used
- `geo_alpha2`: Used to get a proxy for this region, if this and `proxy` are not set, a random proxy will be used.
- `platform`: Used when selecting a proxy if one was not passed in. Will keep from selecting proxies that do not work on a given site.
- `device_type`: used when setting a user-agent if one was not set. Options are `desktop` or `mobile`

## Downloading

Uses a requests.Session to make get and post requests.
The __init__ of the BaseDownload class can take the following args:
- task: needed no matter what
- cookies: dict to set cookies for the whole session. default: none
- headers: dict to set headers for the whole session. default: random User-Agent for the device type, will use desktop if no device type is set)
- proxy: the proxy string to use for the requests


When using BaseDownloader, a requests session is created under `self.session`, so every get/post you make will use the same session per task.
you can also set cookies/headers per call to by pasing the keyword args to self.get() and self.post(). Any kwargs you pass to self.get/post will be pased to the sessions get/post methods.  

When using BaseDownloader's get & post functions, it will use the requests session created in __init__ and return you a Request object. From that, you can acess the raw requests request from `self.r` and the raw source at `self.source` (`r.text`)  

The Request object also gives full access to the WriteTo class (see <here> about what that has)  


A request will retry n (3 by default) times to get a successful status code, each retry it will try and trigger a function called `new_profile()` where you have the chance to switch the headers/cookies/proxy the request is using (will only update for that request?). If that function does not exist, it will try again with the same data.


### Setting headers/cookies/proxies

The ones set in the `self.get/post` will override the ones set in the `__init__`

self.get/post kwargs cookies/headers/proxy
will override
self.task[cookies/headers/proxy]
will override
__init__ kwargs cookies/headers/proxy

Any header/cookie/proxy set on the request (get/post/etc) will only be set for that single request. For those values to be set in the session they must be set from the init or be in the task data.


## Extracting
Coming to a Readme near you...


## Config

3 Ways of setting config values:
- CLI Argument: Will override any other type of config value
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
      type: local  # (local, sns) Default: local
      sns_arn: sns:arn:of:service:to:trigger  # Required if type is sns, if local this is not needed
    ratelimit:
      type: qps  # (qps, period) Required. `qps`: Queries per second to dispatch the tasks at. `period`: The time in hours to dispatch all of the tasks in.
      value: 1  # Required. Can be an int or a float. When using period, value is in hours
```

If you are using the `*file_name` template, a python `.format()` runs on this string so you can use `{key_name}` to make it dynamic. The keys you will have direct access to are the following:
    - All keys in your task that was dispatched
    - Any thing you pass into the `template_values={}` kwarg for the `.save()` fn
    - `time_downloaded`: time (utc) passed from the downloader
    - `date_downloaded`: date (utc) passed from the downloader
    - `time_extracted`: time (utc) passed from the extractor
    - `date_extracted`: date (utc) passed from the extractor

Anything under the `default` section can also have its own value per scraper. So if we have a scraper named `search` and we want it to use a different rate limit then all the other scrapers you can do:
```yaml
search:
  dispatch:
    ratelimit:
      type: period
      value: 5
```
# Change log


### 0.5.1
- Added 'default' as a QA option. If the key is not set in the dict returned by the extractor, it will use the default

### 0.5.0
- Started this change log
- Added `run_id` to all scraper logs. Also to the scrapers config values
- Have all scraper logs pull its extras from `scraper.log_extras()`
- Extraction error logs will have the scrapers correct filename and line number rather then where the library threw the exception
- Fixed bug of s3 endpoint not always getting set correctly for custom endpoints
- Added `pre_extract()` method to extract class, it will run after the `__init__`, used for the user to setup class wide vars
- Added aws access key id & secret override for the `DOWNLOADER` and `EXTRACTOR`, _see config section in readme_

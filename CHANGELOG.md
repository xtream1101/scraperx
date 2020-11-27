# Change log


### 0.5.0
- Started this change log
- Added `run_id` to all scraper logs
- Have all scraper logs pull its extras from `scraper.log_extras()`
- Extraction error logs will have the scrapers correct filename and line number rather then where the library threw the exception
- Fixed bug of s3 endpoint not always getting set correctly for custom endpoints
- Added `pre_extract()` method to extract class, it will run after the `__init__` for the user to use to setup class wide vars

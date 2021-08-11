# Change log

### 0.5.11
- Fixed json encoding error in tests when displaying the `type` of a value

### 0.5.10
- Support for multiple scrapers in a single file, the passed in "scraper_name" will now be used for the config settings. It will still default to the file name if not supplied

### 0.5.9
- Fixed bug when reading in files to compare when running the scrapers unittests

### 0.5.8
- Fixed error when checking the files encoding when using the `create-test` sub command

### 0.5.7
- Fixed encoding detection to work when extracting files from s3
- Added some testing around file encoding and rate limiting

### 0.5.6
- When reading & writing files, use the `cchardet` library to detect the correct file encoding

### 0.5.5
- Fixed reading tests sample data directory on windows now that pathlib is used

### 0.5.4
- Revert of 0.5.3, do NOT ignore the unicode errors. _Will need to find another solution to this when creating tests on both windows and mac/linux_

### 0.5.3
- Ignore unicode errors when reading a file

### 0.5.2
- Updated filepaths in scraper create-test command. Now in windows it will save the path with forward slashes `/`, and not `\\` (supported by pathlib)
- Fixed outdated examples to use a new site

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

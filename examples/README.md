# Example scrapers

These are example scrapers from the most minimal to the very complex.


#### TODO's
  - Order these below to simplist to complex
  - Add doc strings to all fn's in examples
  - Add descriptions to the readme for each example scraper


### Minimal
`minimal.py`  
This scraper demos the least amount of code to have a working scraper


### Multiple Extractors
`multiple_extractors.py`  
Have 2 extractors for a single file  
use extractor_name is saving of the data


### Multiple source files
`multiple_sources.py`
Download the first source, and use that data extracted to download the next. Saving both source files.


### Json source
`json_source.py`  
Downlaod and parse a json file


### QA results
`qa_results.py`  
Set rules around each field and during extraction the check will make sure the extracted data complies with the rules. If not then the extraction fails and no data is saved. The reason is logged as an error.


--------------------------------------------------------------------------------

### Example
Make a post request then a get request right after. This is useful to set cookies or some other data needed to make the 2nd request. Saving only the last request.


### Example
Page through results by re trigging the downloader from the extractor

### Example
trigger to download again from info that was extracted (think best sellers)  
TODO: first refactor the triggering code

### Example
save extracted data as parquet

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


### Json source
`json_source.py`  
Downlaod and parse a json file


### QA results
`qa_results.py`  
Set rules around each field and during extraction the check will make sure the extracted data complies with the rules. If not then the extraction fails and no data is saved. The reason is logged as an error.


--------------------------------------------------------------------------------

### Example
Download 2 files, extract just the 2nd one  
use template_vars in saving of source

### Example
trigger to download again from info that was extracted (think best sellers)  
TODO: first refactor the triggering code

### Example
save extracted data as parquet

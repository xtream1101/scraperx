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


### Scraper multiple pages of data

#### Example 1 - Trigger the download for the next page on extract
`page_trigger_download.py`  
Get multiple pages of product results. Also start the rank at 1 and not 0.  
_The example this scrapers is demoing could be done by dispatching urls with the page in the url, but for the sake of example we are letting the extractor trigger the next page._  
Pros:  
1. It will get as many pages as the site has. Great if its an unknown.
2. Needed if the next page is not just a simple page number in the url, therefore cannot be guessed on dispatch.

Cons:  
1. If a page fails to extract, no other pages will be dispatched.
2. When the scrape starts, there is no way to know how many things it will scrape which makes the ETA unknown.
3. Less control over the exact rate limit of the page downloads.


#### Exmaple 2 - Dispatch all the pages needed
`page_dispatch.py`  
The difference from example 1 is that we are dispatching each page as a task which allows for a few things.  
Pros:  
1. We control the rate of page downloads in a more controlled way
2. If a page fails to extract, other pages are still dispatched so data is only missing for that single page
3. You know how much data to expect as soon as the scrpaer starts

Cons:  
1. If you hardcode a max page to get and there are not that many pages on the site you will get a lot of 404's
2. To be dynamic, you need to first get how many pages the site has, and if that fails then nothing may dispatch


--------------------------------------------------------------------------------


### Example
Make a post request then a get request right after. This is useful to set cookies or some other data needed to make the 2nd request. Saving only the last request.


### Example
save extracted data as parquet

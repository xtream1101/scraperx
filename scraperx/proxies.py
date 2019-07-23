import os
import csv
import random
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def _load_proxies(scraper):
    global proxies
    proxies = defaultdict(list)
    proxy_file = os.getenv('PROXY_FILE')
    if proxy_file and os.path.isfile(proxy_file):
        try:
            logger.info(f"Reading proxy file {proxy_file}",
                        extra={'task': None,
                               'scraper_name': scraper.config['SCRAPER_NAME']})
            with open(proxy_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    country = row['country'].strip().upper()
                    proxies[country].append(row['proxy'].strip())
        except Exception:
            logger.exception("Failed to read proxy file",
                             extra={'task': None,
                                    'scraper_name': scraper.config['SCRAPER_NAME']})

    if not proxies:
        logger.debug("No proxy list to choose from",
                     extra={'task': None,
                            'scraper_name': scraper.config['SCRAPER_NAME']})


def get_proxy(scraper, country=None):
    """Get a proxy from the proxy file if set

    Set the env var `PROXY_FILE` to a csv that has the header `country,proxy`
    Get a random proxy from the proxy file based on the `country` passed in

    Args:
        scraper (obj): Users Scraper instance. Used to know which scraper is trying to load proxies.
        country (str, optional): 2 letter country code to get the proxy for.
            If None it will get any proxy. Defaults to None.

    Returns:
        str|None: Full proxy url or None if no proxy is found.
    """
    global proxies
    try:
        proxies
    except NameError:
        # Proxies have not been loaded yet
        _load_proxies(scraper)

    if not proxies:
        return None

    available_proxies = []
    if country is not None:
        available_proxies = proxies.get(country.upper(), [])
    else:
        # Get all proxies
        for country_proxies in proxies.values():
            available_proxies.extend(country_proxies)

    if available_proxies:
        return random.choice(available_proxies)

    return None

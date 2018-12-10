import os
import csv
import random
import logging
from collections import defaultdict


logger = logging.getLogger(__name__)


proxies = defaultdict(list)
proxy_file = os.getenv('PROXY_FILE')
if proxy_file and os.path.isfile(proxy_file):
    try:
        logger.info(f"Reading proxy file {proxy_file}")
        with open(proxy_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                proxies[row['country'].strip().upper()].append(row['proxy'].strip())
    except Exception:
        logger.exception("Failed to read proxy file")


def get_proxy(country=None):
    """Get a proxy string to use for the request

    Keyword Arguments:
        country {str} -- ISO alpha2 country code. (default: {None})

    Returns:
        str/None -- The proxy string (or None) of the choosen proxy
    """
    if not proxies:
        return None

    country = country.upper() if country is not None else country

    available_proxies = []
    if country:
        available_proxies = proxies.get(country, [])
    else:
        # Get all proxies
        for country_proxies in proxies.values():
            available_proxies.extend(country_proxies)

    if available_proxies:
        return random.choice(available_proxies)

    return None


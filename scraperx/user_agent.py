import os
import csv
import random
import logging
from collections import defaultdict

from .config import config
logger = logging.getLogger(__name__)


DEFAULT_USER_AGENTS = {
    "desktop": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38",  # noqa: E501
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063",  # noqa: E501
    ],
    "mobile": [
        "Mozilla/5.0 (Linux; Android 7.0; SM-G955U Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.81 Mobile Safari/537.36",  # noqa: E501
        "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile Safari/602.1",  # noqa: E501
        "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/603.1.23 (KHTML, like Gecko) Version/10.0 Mobile Safari/602.1",  # noqa: E501
    ],
}


def _load_user_agents():
    global user_agents
    user_agents = defaultdict(list)
    ua_file = os.getenv('UA_FILE')
    if ua_file and os.path.isfile(ua_file):
        try:
            logger.info(f"Reading user agent file {ua_file}",
                        extra={'task': None,
                               'scraper_name': config['SCRAPER_NAME']})
            with open(ua_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    device_type = row['device_type'].strip().lower()
                    user_agents[device_type].append(row['user_agent'].strip())
        except Exception:
            logger.exception("Failed to read user agent file",
                             extra={'task': None,
                                    'scraper_name': config['SCRAPER_NAME']})

    if not user_agents:
        logger.debug("No user agents to choose from. Loading defaults",
                     extra={'task': None,
                            'scraper_name': config['SCRAPER_NAME']})
        user_agents = DEFAULT_USER_AGENTS


def get_user_agent(device_type='desktop'):
    """Get a user-agent to use for the request

    Keyword Arguments:
        device_type {str} -- The device the user-agent string is for (default: {desktop})

    Returns:
        str -- The user-agent string
    """
    global user_agents
    try:
        user_agents
    except NameError:
        # User-Agents have not been loaded yet
        _load_user_agents()

    if device_type is None:
        # This extra check is here to make sure the default is really desktop
        device_type = 'desktop'

    device_type = device_type.lower()
    if device_type not in user_agents:
        raise ValueError(f"No user-agents found for device type: {device_type}")

    return random.choice(user_agents[device_type])

import logging

logger = logging.getLogger(__name__)


def get_proxy(alpha2=None, platform=None):
    """Get a proxy string to use for the request

    Keyword Arguments:
        alpha2 {str} -- ISO alpha2 country code. (default: {None})
        platform {str} -- [description] (default: {None})

    Returns:
        str/None -- The proxy string (or None) of the choosen proxy
    """
    return None
    # if proxies is None:
    #     return None

    # alpha2 = alpha2.upper() if alpha2 is not None else alpha2
    # platform = platform.lower() if platform is not None else platform

import requests.exceptions


class QAValueError(ValueError):
    pass


class DownloadValueError(ValueError):
    pass


class HTTPIgnoreCodeError(requests.exceptions.RequestException):
    """Requests exception for ignore_codes"""
    pass

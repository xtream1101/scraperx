import requests.exceptions


class QAValueError(ValueError):
    """Raised if extracted data failed to pass its qa tests.
    Based on the `qa` keyword argument passed into the `extract_task`
    """
    pass


class DownloadValueError(ValueError):
    """Raised if the download still failed after re-tries"""
    pass


class HTTPIgnoreCodeError(requests.exceptions.RequestException):
    """Requests exception for ignore_codes"""
    pass

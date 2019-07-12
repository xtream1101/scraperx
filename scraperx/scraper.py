import logging
from .config import ConfigGen
from .dispatch import Dispatch
from .download import Download

logger = logging.getLogger(__name__)


class Scraper:

    def __init__(self, config_file=None, cli_args=None, scraper_name=None,
                 dispatch_cls=None, download_cls=None, extract_cls=None):
        self.config = ConfigGen(config_file=config_file,
                                cli_args=cli_args,
                                scraper_name=scraper_name)
        self.set_download_cls(download_cls)
        self.set_dispatch_cls(dispatch_cls)
        self.set_extract_cls(extract_cls)

    def set_download_cls(self, download_cls):
        if download_cls:
            self.download_cls = download_cls
        else:
            self.download_cls = Download

    def set_dispatch_cls(self, dispatch_cls):
        if dispatch_cls:
            self.dispatch_cls = dispatch_cls
        else:
            self.dispatch_cls = Dispatch

    def set_extract_cls(self, extract_cls):
        self.extract_cls = extract_cls

    def extract(self, *args, **kwargs):
        if self.extract_cls is not None:
            return self.extract_cls(self, *args, **kwargs)

    def dispatch(self, *args, **kwargs):
        return self.dispatch_cls(self, *args, **kwargs)

    def download(self, *args, **kwargs):
        return self.download_cls(self, *args, **kwargs)

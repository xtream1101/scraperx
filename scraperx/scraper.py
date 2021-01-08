import logging
from .config import ConfigGen
from .dispatch import Dispatch
from .download import Download

logger = logging.getLogger(__name__)


class Scraper:

    def __init__(self, config_file=None, cli_args=None, scraper_name=None,
                 dispatch_cls=None, download_cls=None, extract_cls=None):
        """Creates a Scraper object of the users custom classes

        Args:
            config_file (str, optional): Path to a config file. Can be set later
                in self.config.load_config(). Defaults to None.
            cli_args (argparse, optional): CLI arguments to be passed in. Can be set later
                in self.config.load_config(). Defaults to None.
            scraper_name (str, optional): Override the default scraper name which is the file name.
                Can be set later in self.config.load_config(). Defaults to None.
            dispatch_cls (obj, optional): Users scraper Dispatch class. Defaults to None.
            download_cls (obj, optional): Users scraper Download class. Defaults to None.
            extract_cls (obj, optional): Users scraper Extract class. Defaults to None.
        """
        self.config = ConfigGen(config_file=config_file,
                                cli_args=cli_args,
                                scraper_name=scraper_name)
        self._set_download_cls(download_cls)
        self._set_dispatch_cls(dispatch_cls)
        self._set_extract_cls(extract_cls)

    def log_extras(self):
        """Extra data to always add to log messages

        Returns:
            dict: Data that should go into the extras kwarg of all log messages
        """
        return {
            'scraper_name': self.config['SCRAPER_NAME'],
            'run_id': self.config['RUN_ID'],
        }

    def _set_download_cls(self, download_cls):
        if download_cls:
            self.download_cls = download_cls
        else:
            self.download_cls = Download

    def _set_dispatch_cls(self, dispatch_cls):
        if dispatch_cls:
            self.dispatch_cls = dispatch_cls
        else:
            self.dispatch_cls = Dispatch

    def _set_extract_cls(self, extract_cls):
        self.extract_cls = extract_cls

    def extract(self, *args, **kwargs):
        if self.extract_cls is not None:
            return self.extract_cls(self, *args, **kwargs)

    def dispatch(self, *args, **kwargs):
        return self.dispatch_cls(self, *args, **kwargs)

    def download(self, *args, **kwargs):
        return self.download_cls(self, *args, **kwargs)

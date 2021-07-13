import os
import sys
import uuid
import yaml
import logging

logger = logging.getLogger(__name__)


def _make_float(value):
    """Convert the value to a float
    Needed if the value is "1/2"

    Args:
        value (str): The value to convert

    Returns:
        float: value as a float
    """
    value = str(value)
    if '/' in value:
        return (float(value.split('/')[0])
                / float(value.split('/')[1]))
    else:
        return float(value)


# All config keys must be UPPERCASE
_CONFIG_STRUCTURE = {
    # 'EXAMPLE': {  # Config value name
    #     'default': 2,  # Optional: Will use if not cli_arg|yaml|env is set
    #     'type': int,  # Required: Python type to enforce/cast value to be
    #     'must_be': [1, 2, 3],  # Optional: The value must be in this list
    #     'transformer': some_fn,  # Optional: Pass the value through this function and use its return  # noqa
    #     'required_if':  # Optional: Forces this value required or not based on other values  # noqa
    #         # Option 1
    #         # Config key that needs to be set to not None
    #         'foo_bar',
    #         # Option 2
    #         # Dict of a config key & value that it needs to be set to
    #         {'FOO_BAR': 'hello'},
    #         # Option 3
    #         # Dict of a config key & a list of values it could be set to
    #         {'FOO_BAR': ['hello', 'world']},
    #         # Option 4
    #         # List of the above options is allowed too
    #         [
    #             'TEST_KEY',
    #             {'FOO_BAR': ['hello', 'world']},
    #         ],
    # },

    ###
    # Other
    ##
    'STANDALONE': {
        'default': False,
        'type': bool,
    },
    'SCRAPER_NAME': {
        'type': str,
    },
    'RUN_ID': {
        'type': str,
        'default': str(uuid.uuid4()),
    },
    ###
    # Dispatch
    ###
    'DISPATCH_SERVICE_NAME': {
        'default': 'local',
        'type': str,
        'must_be': ['local', 'sns'],
    },
    'DISPATCH_SERVICE_SNS_ARN': {
        'type': str,
        'required_if': {'DISPATCH_SERVICE_NAME': 'sns'},
    },
    'DISPATCH_RATELIMIT_TYPE': {
        'type': str,
        'default': 'qps',
        'must_be': ['period', 'qps'],
    },
    'DISPATCH_RATELIMIT_VALUE': {
        'type': float,
        'default': 1.0,
        'transformer': _make_float,
    },
    'DISPATCH_LIMIT': {
        'type': int,
    },
    ###
    # Downloader
    ###
    'DOWNLOADER_SAVE_DATA_SERVICE': {
        'type': str,
        'default': 'local',
        'must_be': ['local', 's3'],
    },
    'DOWNLOADER_SAVE_DATA_BUCKET_NAME': {
        'type': str,
        'required_if': {'DOWNLOADER_SAVE_DATA_SERVICE': 's3'},
    },
    'DOWNLOADER_SAVE_DATA_ENDPOINT_URL': {
        'default': None,
        'type': str,
    },
    'DOWNLOADER_SAVE_DATA_AWS_ACCESS_KEY_ID': {
        'type': str,
    },
    'DOWNLOADER_SAVE_DATA_AWS_SECRET_ACCESS_KEY': {
        'type': str,
    },
    'DOWNLOADER_SAVE_METADATA': {
        'default': True,
        'type': bool,
    },
    'DOWNLOADER_FILE_TEMPLATE': {
        'default': "output/source.html",
        'type': str,
    },
    ###
    # Extractor
    # TODO: Uses same as downloader, best way?
    ###
    'EXTRACTOR_SAVE_DATA_SERVICE': {
        'type': str,
        'default': 'local',
        'must_be': ['local', 's3'],
    },
    'EXTRACTOR_SAVE_DATA_BUCKET_NAME': {
        'type': str,
        'required_if': {'EXTRACTOR_SAVE_DATA_SERVICE': 's3'},
    },
    'EXTRACTOR_SAVE_DATA_ENDPOINT_URL': {
        'default': None,
        'type': str,
    },
    'EXTRACTOR_SAVE_DATA_AWS_ACCESS_KEY_ID': {
        'type': str,
    },
    'EXTRACTOR_SAVE_DATA_AWS_SECRET_ACCESS_KEY': {
        'type': str,
    },
    'EXTRACTOR_FILE_TEMPLATE': {
        'default': "output/extracted.json",
        'type': str,
    },
}


class ConfigGen:

    def __init__(self, config_file=None, cli_args=None, scraper_name=None):
        """Creates a config instance for a scraper

        All arguments can be set later using self.load_config()

        Args:
            config_file (str, optional): Path to the config file. If not set, the default
                path of `./config.yaml` will be used. Defaults to None.
            cli_args (argparse, optional): Arguments from scraperx.arguments That can override
                all config values. Defaults to None.
            scraper_name (str, optional): Use this as the scraper name. If not set the filename
                is used. Defaults to None.
        """
        self.values = {}
        self._cli_args = cli_args
        self._scraper_file_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
        if scraper_name:
            self._scraper_name = scraper_name
        else:
            self._scraper_name = self._scraper_file_name

        if config_file:
            self._file = config_file
        else:
            self._file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])),
                                      'config.yaml')

        # Do an initial load of current config
        self.load_config()

    def load_config(self, config_file=None, cli_args=None, scraper_name=None):
        """Load the config values for a scraper

        Args:
            config_file (str, optional): Path to the config file. If not set, the default
                path of `./config.yaml` will be used. Defaults to None.
            cli_args (argparse, optional): Arguments from scraperx.arguments That can override
                all config values. Defaults to None.
            scraper_name (str, optional): Use this as the scraper name. If not set the filename
                is used. Defaults to None.
        """
        if config_file:
            self._file = config_file

        if scraper_name is not None:
            self._scraper_name = scraper_name

        if not cli_args:
            cli_args = self._cli_args

        try:
            file_values = self._ingest_file(self._file)
        except FileNotFoundError as e:
            logger.error(str(e))
            file_values = {}

        cli_values = self._ingest_cli_args(cli_args)

        raw_values = self._join_values(file_values=file_values,
                                       cli_values=cli_values)

        if raw_values['SCRAPER_NAME'] is None:
            raw_values.update({'SCRAPER_NAME': self._scraper_name})

        self.values = self._validate_config_values(raw_values)

    def __getitem__(self, key):
        """Get the config value from the class via config['KEYNAME']

        Args:
            key (str): The key to get from the config values

        Returns:
            str/None: Value of the key passed in, or None if not found
        """
        return self.values.get(key.upper())

    def _set_value(self, key, value):
        """Only the test will need to change some values at runtime

        Nothing else should use this except tests, which is why its private
        TODO: Is there a better way for tests then doing this?

        Args:
            key (str): config key to change
            value (str): value to set
        """
        self.values[key.upper()] = value

    def _join_values(self, file_values, cli_values):
        """Join the values from the config file, cli args, and env vars

        Args:
            file_values (dict): Values from the config file
            cli_values (dict) Values from the command line

        Returns:
            dict: A single value for each config key
        """
        final_config = {}
        for key, struct in _CONFIG_STRUCTURE.items():
            if key in cli_values:
                # 1st check cli_args
                final_config[key] = cli_values[key]

            elif key in os.environ:
                # 2nd check env variables
                final_config[key] = os.getenv(key)

            elif key in file_values:
                # 3rd check the config file
                final_config[key] = file_values[key]

            else:
                # Use default value if it has one
                final_config[key] = struct.get('default')

        return final_config

    def _validate_config_values(self, raw_values):
        """Validate the config values

        Args:
            raw_values (dict): flattened keys with their values

        Returns:
            dict: Values that have been validated and cast
        """
        validated_values = {}

        for key, struct in _CONFIG_STRUCTURE.items():
            value = raw_values.get(key)

            ###
            # Required If
            ###
            if 'required_if' in struct:
                required_if = struct['required_if']
                if not isinstance(required_if, (list, tuple)):
                    # Could be a single item or a list
                    # Make into a list so the checks work the same
                    required_if = [required_if]

                for item in required_if:
                    if isinstance(item, str):
                        # Only care if required key is not None
                        if raw_values[item] is not None and value is None:
                            raise ValueError((f"Config key {key} is required "
                                              f"since {item} is set"))

                    elif isinstance(item, dict):
                        # Care about what the value of the key is set to
                        required_key = list(item.keys())[0]
                        possible_values = item[required_key]
                        if isinstance(possible_values, str):
                            # Could be a single item or a list
                            # Make into a list so the checks work the same
                            possible_values = [possible_values]

                        required_value = raw_values[required_key]
                        if required_value in possible_values and value is None:
                            err = (f"Config key {key} is required if"
                                   f" {required_key} is {required_value}")
                            logger.critical(err,
                                            extra={'task': None,
                                                   'scraper_name': self._scraper_name})  # noqa E501
                            sys.exit(1)

                if value is None:
                    # If the value is None and nothing was raised
                    # then we do not care about this value so move on
                    continue

            ###
            # Transform
            ###
            if 'transformer' in struct:
                value = struct['transformer'](value)

            ###
            # Must Be
            ###
            if 'must_be' in struct:
                if value not in struct['must_be']:
                    err = (f"Config value for {key} can only be these values:"
                           f" {struct['must_be']}. Current: {value}")
                    logger.critical(err,
                                    extra={'task': None,
                                           'scraper_name': self._scraper_name})
                    sys.exit(1)

            ###
            # Type
            ###
            try:
                if value is not None:
                    value = struct['type'](value)
            except ValueError:
                err = (f"Config value for {key} must be the type"
                       f" {struct['type'].__name__}")
                logger.critical(err,
                                extra={'task': None,
                                       'scraper_name': self._scraper_name})
                sys.exit(1)

            validated_values[key] = value

        return validated_values

    def _ingest_file(self, config_file):
        current_config = {}
        with open(config_file, 'r') as stream:
            try:
                all_config_values = yaml.load(stream, Loader=yaml.FullLoader)
            except AttributeError:
                # Version of pyyaml that does not have yaml.FullLoader
                all_config_values = yaml.load(stream)
            default_config_raw = all_config_values.get('default', {})
            current_config.update(ConfigGen._flatten(default_config_raw))
            # Get scraper values
            # Use the custom scraper name if given
            scraper_config_raw = all_config_values.get(self._scraper_name, {})
            current_config.update(ConfigGen._flatten(scraper_config_raw))

        return current_config

    @staticmethod
    def _flatten(dict_obj, prev_key='', sep='_'):
        """Take a nested dict and un-nest all values

        Args:
            dict_obj (dict): The dict that needs to be flattened
            prev_key (str, optional): Used for recursion. Defaults to ''.
            sep (str, optional): How the nested keys should be seperated. Defaults to '_'.

        Returns:
            dict: All values are now just one key away
        """
        items = {}
        for key, value in dict_obj.items():
            new_key = prev_key + sep + key if prev_key != '' else key

            new_key = new_key.upper()
            if isinstance(value, dict):
                items.update(ConfigGen._flatten(value, new_key))
            else:
                items[new_key] = value

        return items

    def _ingest_cli_args(self, cli_args):
        """Map the cli arguments to the correct flattened key

        Only return the keys that have values.
        This is so that `None` can be a valid overide type if needed since we
        can check if the key is even set or not.

        Set the key without default/scraper_name as prefix here

        Args:
            cli_args (argparser): The cli arguments from argparser

        Returns:
            dict: Dict of flatten config keys that each cli argument maps to
        """
        cli_config = {}

        try:
            if cli_args.local:
                cli_config['DISPATCH_SERVICE_NAME'] = 'local'
                cli_config['DOWNLOADER_SAVE_DATA_SERVICE'] = 'local'
                cli_config['EXTRACTOR_SAVE_DATA_SERVICE'] = 'local'
        except AttributeError:
            pass

        try:
            if cli_args.standalone:
                cli_config['STANDALONE'] = cli_args.standalone
        except AttributeError:
            pass

        try:
            if cli_args.limit:
                cli_config['DISPATCH_LIMIT'] = cli_args.limit
        except AttributeError:
            pass

        try:
            if cli_args.qps:
                cli_config['DISPATCH_RATELIMIT_TYPE'] = 'qps'
                cli_config['DISPATCH_RATELIMIT_VALUE'] = cli_args.qps
        except AttributeError:
            pass

        try:
            if cli_args.period:
                cli_config['DISPATCH_RATELIMIT_TYPE'] = 'period'
                cli_config['DISPATCH_RATELIMIT_VALUE'] = cli_args.period
        except AttributeError:
            pass

        return cli_config

import os
import sys
import yaml
import logging

logger = logging.getLogger(__name__)


class Config:

    def __init__(self, scraper_name, config_file, cli_args=None):
        self.scraper_name = scraper_name
        try:
            self.config = Config.get_config_from_file(config_file,
                                                      cli_args=cli_args)
        except ValueError as e:
            logger.critical(f"Invalid config \n{e}")
            # Should not continue if there is an issue with the config
            sys.exit(1)

    def get(self, config_key):
        """Get the config value for a key

        Check to see if the scraper has a value for the config_key
        if not then use the default value.

        Arguments:
            config_key {str} -- Key name to get the value for.
                                Should not contain the scraper name or default.
                                This will check the correct locations.

        Returns:
            any -- The value of the config_key

        Raises:
            ValueError -- if the config_key does not exist
        """
        try:
            # Try and get scraper specific value first
            key = '{}_{}'.format(self.scraper_name, config_key)
            value = self.config[key.upper()]
        except KeyError:
            key = 'default_{}'.format(config_key)
            try:
                value = self.config[key.upper()]
            except KeyError:
                raise ValueError(f"No config value {key.upper()}")

        return value

    @staticmethod
    def get_config_from_file(config_file, cli_args=None):
        """Get all config values needed to run the scrape

        Will handle reading in the scrapers config and updating with
        any env vars or cli args as needed

        Arguments:
            config_file {str} -- The path for the scrapers config file

        Keyword Arguments:
            cli_args {argparse.Namespace} -- Any cli args that were set at
                                             runtime (default: {None})

        Returns:
            dict -- The finial config keys and their values
        """
        config_raw = Config._ingest_config_file(config_file)
        config_raw = Config._update_config_values(config_raw, cli_args=cli_args)
        return Config._validate_config_values(config_raw)

    @staticmethod
    def _ingest_config_file(config_file):
        """Read in config yaml file

        Arguments:
            config_file {str} -- Scrapers config file

        Returns:
            dict -- Dict of config keys flattened using underscores
        """
        config_flat_raw = {}
        with open(config_file, 'r') as stream:
            config_flat_raw = Config.flatten(yaml.load(stream))
        return config_flat_raw

    @staticmethod
    def _update_config_values(config_flat_raw, cli_args=None):
        """Update missing and override config vaules if needed

        Override the config.yaml values with any environment vars that are set.
        Override then with any cli args that were passed in at runtime

        Arguments:
            config_flat_raw {dict} -- Flattened config values from the
                                      scrapers config

        Keyword Arguments:
            cli_args {argparse.Namespace} -- Any cli args that were set at
                                             runtime (default: {None})

        Returns:
            dict -- Updated config values
        """
        # Contains all values that are in the scrapers yaml
        config = config_flat_raw.copy()

        for c_key in config.keys():
            # Check to see if any env vars exist for each config value
            if c_key in os.environ:
                config[c_key] = os.getenv(c_key)

            # Override any config values with cli args
            if cli_args:
                ###
                # For Dispatcher
                ###
                if hasattr(cli_args, 'limit') and cli_args.limit:
                    if c_key.endswith('DISPATCH_LIMIT'):
                        config[c_key] = cli_args.limit
                        # Set default here so it does not get defaulted below
                        config['DEFAULT_DISPATCH_LIMIT'] = cli_args.limit

                if hasattr(cli_args, 'qps') and cli_args.qps:
                    if c_key.endswith('DISPATCH_RATELIMIT_TYPE'):
                        config[c_key] = 'qps'
                    elif c_key.endswith('DISPATCH_RATELIMIT_VALUE'):
                        config[c_key] = cli_args.qps

                if hasattr(cli_args, 'period') and cli_args.period:
                    if c_key.endswith('DISPATCH_RATELIMIT_TYPE'):
                        config[c_key] = 'period'
                    elif c_key.endswith('DISPATCH_RATELIMIT_VALUE'):
                        config[c_key] = cli_args.period

                ###
                # For Downloader
                ###
                # TODO

                ###
                # For Extractor
                ###
                # TODO

        # Set defaults for values that are not set and have default options
        defaults = {'DEFAULT_EXTRACTOR_SAVE_DATA_SERVICE': 'local',
                    'DEFAULT_DOWNLOADER_SAVE_DATA_SERVICE': 'local',
                    'DEFAULT_DISPATCH_SERVICE_TYPE': 'local',
                    'DEFAULT_DISPATCH_LIMIT': None,
                    }
        for d_key, d_value in defaults.items():
            if d_key not in config:
                config[d_key] = d_value

        return config

    @staticmethod
    def _validate_config_values(config):
        """Validate the config values

        Make sure all the required values are here and they have the correct
        options set.
        Will convert the types of some value to make sure they are correct

        Arguments:
            config {dict} -- The flattened and updated values

        Returns:
            dict -- The finial config keys and their values

        Raises:
            ValueError -- If any of the config values are invalid
        """
        issues = []  # Get all the issues before raising the exception

        required_fields = ['DEFAULT_DISPATCH_RATELIMIT_VALUE',
                           'DEFAULT_DISPATCH_SERVICE_TYPE',
                           'DEFAULT_DOWNLOADER_SAVE_DATA_SERVICE',
                           'DEFAULT_EXTRACTOR_SAVE_DATA_SERVICE',
                           ]
        # Check if required values are missing
        for key in required_fields:
            if config.get(key) is None:
                issues.append(f"Config key {key} is required")

        # Check that the values set are valid
        for key, value in config.items():
            if key.endswith('DISPATCH_LIMIT'):
                if value is not None and not isinstance(value, int):
                    issues.append(f"{key} must be not set or an int")
                elif value is not None and value <= 0:
                    issues.append(f"{key} must be greater then 0")

            if key.endswith('DISPATCH_RATELIMIT_TYPE'):
                if value not in ('qps', 'period'):
                    issues.append(f"{key} must be qps or period")

            if key.endswith('DISPATCH_RATELIMIT_VALUE'):
                try:
                    config[key] = float(value)
                except (ValueError, TypeError):
                    issues.append(f"{key} must be an int or float")

                if config[key] <= 0:
                    issues.append(f"{key} must be greater then 0")

            if key.endswith('DISPATCH_SERVICE_TYPE'):
                if value not in ('local', 'sns'):
                    issues.append(f"{key} must be local or sns")
                elif value == 'sns':
                    # Also check that DISPATCH_SERVICE_SNS_ARN is set
                    default_sns_arn = 'DEFAULT_DISPATCH_SERVICE_SNS_ARN'
                    this_sns_arn = key.replace('TYPE', 'SNS_ARN')
                    if (not config.get(default_sns_arn)
                            and not config.get(this_sns_arn)):
                        message = f"{default_sns_arn}"
                        if default_sns_arn != this_sns_arn:
                            message += f" or {this_sns_arn}"
                        issues.append(f"{message} must be set")

            if key.endswith('SAVE_DATA_SERVICE'):
                if value not in ('local', 's3'):
                    issues.append(f"{key} must be local or s3")
                elif value == 's3':
                    # Also check that SAVE_DATA_BUCKET_NAME is set
                    key_type = key.split('_SAVE_DATA')[0].split('_')[-1]
                    default_bucket = f'DEFAULT_{key_type}_SAVE_DATA_BUCKET_NAME'
                    this_buckey = key.replace('SERVICE', 'BUCKET_NAME')
                    if (not config.get(default_bucket)
                            and not config.get(this_buckey)):
                        message = f"{default_bucket}"
                        if default_bucket != this_buckey:
                            message += f" or {this_buckey}"
                        issues.append(f"{message} must be set")

        if issues:
            formatted_issues = '\n'.join(issues)
            raise ValueError(f"Invalid config: \n{formatted_issues}")

        return config

    @staticmethod
    def flatten(dict_obj, prev_key='', sep='_'):
        """Take a nested dict and un nest all values

        Arguments:
            dict_obj {dict} -- The dict that needs to be flattened

        Keyword Arguments:
            prev_key {str} -- Used for recursion (default: {''})
            sep {str} -- How the nested keys should be seperated
                         (default: {'_'})

        Returns:
            dict -- All values are now just one key away
        """
        items = {}
        for key, value in dict_obj.items():
            new_key = prev_key + sep + key if prev_key != '' else key

            new_key = new_key.upper()
            if isinstance(value, dict):
                items.update(Config.flatten(value, new_key))
            else:
                items[new_key] = value

        return items

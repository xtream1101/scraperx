import os
import pathlib
import logging

from . import config
from .utils import get_s3_resource, get_context_type

logger = logging.getLogger(__name__)


class SaveTo:

    def __init__(self, raw_data, content_type='text/html',
                 file_ext=''):
        self.raw_data = raw_data
        self.content_type = content_type
        self.file_ext = file_ext

    def _get_filename(self, context, template_values={}, name_template=None):
        """Generate the filename based on the config template

        Arguments:
            context {class} -- Either the BaseDownload or BaseExtractor class.
                               Used to get timestamps and the correct template.

        Keyword Arguments:
            template_values {dict} -- Additional keys to use in the template
                                      (default: {{}})

        Returns:
            str -- The filename with the template values filled in
        """
        context_type = get_context_type(context)
        additional_args = {'scraper_name': config['SCRAPER_NAME']}
        if context_type == 'extractor':
            time_downloaded = context.download_manifest['time_downloaded']
            date_downloaded = context.download_manifest['date_downloaded']
            additional_args.update({'time_extracted': context.time_extracted,
                                    'date_extracted': context.date_extracted,
                                    'time_downloaded': time_downloaded,
                                    'date_downloaded': date_downloaded,
                                    })
        elif context_type == 'downloader':
            additional_args.update({'time_downloaded': context.time_downloaded,
                                    'date_downloaded': context.date_downloaded,
                                    })

        if not name_template:
            name_template = config[f'{context_type}_FILE_TEMPLATE']

        task_safe = {k: str(v) for k, v in context.task.items()}
        filename = name_template.format(**task_safe,
                                        **template_values,
                                        **additional_args)

        return filename

    def save(self, context, template_values={}, filename=None, metadata=None):
        """Save the file based on the config

        Arguments:
            context {class} -- Either the BaseDownload or BaseExtractor class.

        Keyword Arguments:
            template_values {dict} -- Additional keys to use in the template
                                      (default: {{}})
            filename {str} -- Filename to use rather then the template
                              (default: {None})
            metadata {dict} -- Data to be saved into the s3 file
                               (default: {None})

        Returns:
            str -- File path to where it was saved
        """
        context_type = get_context_type(context)

        filename = self._get_filename(context,
                                      template_values=template_values,
                                      name_template=filename)

        if not filename.endswith(self.file_ext):
            filename += f'.{self.file_ext}'

        save_service_key = f'{context_type}_SAVE_DATA_SERVICE'
        save_service = config[save_service_key]
        if save_service == 's3':
            saved_file = SaveS3(self.raw_data,
                                filename,
                                context,
                                metadata=metadata,
                                content_type=self.content_type).save()

        elif save_service == 'local':
            saved_file = self.save_local(filename)

        else:
            logger.error(f"Not configured to save to {save_service}",
                         extra={'task': context.task,
                                'scraper_name': config['SCRAPER_NAME']})
            saved_file = None

        logger.debug(f"Saved file",
                     extra={'task': context.task,
                            'scraper_name': config['SCRAPER_NAME'],
                            'file': saved_file})
        return saved_file

    def save_local(self, filename):
        return SaveLocal(self.raw_data, filename).save()


class SaveLocal:

    def __init__(self, data, filename):
        self.data = data
        self.filename = filename

    def save(self):
        file_path = os.path.dirname(self.filename)
        pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)

        try:
            with open(self.filename, 'w') as outfile:
                outfile.write(self.data.read())
        except TypeError:
            self.data.seek(0)
            # raw_data is BytesIO not StringIO
            with open(self.filename, 'wb') as outfile:
                outfile.write(self.data.read())

        return self.filename


class SaveS3:

    def __init__(self, data, filename, context, metadata=None,
                 content_type=None):
        self.body = self._format_body(data)
        self.filename = filename
        self.context = context
        self.metadata = self._format_metadata(metadata)
        self.content_type = content_type

        self.s3 = get_s3_resource(context)

    def _get_bucket_name(self):
        context_type = get_context_type(self.context)
        bucket_name_key = f'{context_type}_SAVE_DATA_BUCKET_NAME'
        return config[bucket_name_key]

    def _format_body(self, data):
        try:
            return data.getvalue().encode()
        except AttributeError:
            return data.read()

    def _format_metadata(self, metadata):
        """Make sure all values in the metadata are strings

        For s3 files the values must be strings

        Arguments:
            metadata {dict} -- The metadat to save

        Returns:
            dict -- The metadata that gets written into the file
        """
        if metadata is None:
            metadata = {}

        # Need to convert all values to strings to save as metadata in the file
        for k, v in metadata.items():
            metadata[k] = str(v)

        return metadata

    def save(self):
        bucket = self._get_bucket_name()

        response = self.s3.Object(bucket, self.filename)\
                          .put(Body=self.body,
                               ContentType=self.content_type,
                               Metadata=self.metadata)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error(f"S3 upload response: {response}",
                         extra={'task': self.context.task,
                                'scraper_name': config['SCRAPER_NAME']})
        else:
            logger.debug(f"S3 upload response: {response}",
                         extra={'task': self.context.task,
                                'scraper_name': config['SCRAPER_NAME']})

        return f"s3://{bucket}/{self.filename}"

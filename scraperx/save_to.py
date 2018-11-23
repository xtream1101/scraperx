import os
import pathlib
import logging
from .utils import get_s3_resource, _get_context_type

logger = logging.getLogger(__name__)


class SaveTo:

    def __init__(self, raw_data, content_type='text/html'):
        self.raw_data = raw_data
        self.content_type = content_type

    def _get_filename(self, context, template_values={}):
        """Generate the filename based on the config template

        Arguments:
            context {class} -- Either the BaseDownload or BaseExtractor class.
                               Used to get timestamps and the correct template.

        Keyword Arguments:
            template_values {dict} -- Additonal keys to use in the template
                                      (default: {{}})

        Returns:
            str -- The filename with the template values filled in
        """
        context_type = _get_context_type(context)
        additional_args = {}
        if context_type == 'extractor':
            time_downloaded = context.download_results['time_downloaded']
            date_downloaded = context.download_results['date_downloaded']
            additional_args = {'time_extracted': context.time_extracted,
                               'date_extracted': context.date_extracted,
                               'time_downloaded': time_downloaded,
                               'date_downloaded': date_downloaded,
                               }
        elif context_type == 'downloader':
            additional_args = {'time_downloaded': context.time_downloaded,
                               'date_downloaded': context.date_downloaded,
                               }

        filename = context.config.get(f'{context_type}_FILE_TEMPLATE')\
                                 .format(**context.task,
                                         **template_values,
                                         **additional_args)

        return filename

    def save(self, context, template_values={}, filename=None, metadata=None):
        """Save the file based on the config

        Arguments:
            context {class} -- Either the BaseDownload or BaseExtractor class.

        Keyword Arguments:
            template_values {dict} -- Additonal keys to use in the template
                                      (default: {{}})
            filename {str} -- Filename to use rather then the template
                              (default: {None})
            metadata {dict} -- Data to be saved into the s3 file
                               (default: {None})

        Returns:
            str -- File path to where it was saved
        """
        context_type = _get_context_type(context)

        if not filename:
            filename = self._get_filename(context, template_values)

        save_service = context.config.get(f'{context_type}_SAVE_DATA_SERVICE')
        if (save_service == 's3'
                or context.config.get('DISPATCH_SERVICE_TYPE') == 'sns'):
            bucket_name_key = f'{context_type}_SAVE_DATA_BUCKET_NAME'
            s3 = get_s3_resource(context)
            return self.save_s3(s3,
                                context.config.get(bucket_name_key),
                                filename,
                                metadata=metadata)

        elif save_service == 'local':
            return self.save_local(filename)

        else:
            logger.error(f"Not configured to save to {save_service}")

    def save_local(self, filename):
        """Save the file to the local file system

        Arguments:
            filename {str} -- The location to save the file to

        Returns:
            str -- The location the file was saved to
        """
        file_path = os.path.dirname(filename)
        pathlib.Path(file_path).mkdir(parents=True, exist_ok=True)

        try:
            with open(filename, 'w') as outfile:
                    outfile.write(self.raw_data.read())
        except TypeError:
            # raw_data is BytesIO not StringIO
            with open(filename, 'wb') as outfile:
                outfile.write(self.raw_data.read())

        return filename

    def save_s3(self, s3, bucket, filename, metadata=None):
        """Save the file to an s3 bucket

        Arguments:
            s3 {boto3.resource} -- s3 resource from boto3
            bucket {str} -- Name of the bucket
            filename {str} -- The s3 key to save the data to

        Keyword Arguments:
            metadata {dict} -- The data to save into the s3 file
                               (default: {None})

        Returns:
            str -- bucket/key where the data was saved
        """
        if metadata is None:
            metadata = {}

        # Need to convert all values to strings to save as metadata in the file
        for k, v in metadata.items():
            metadata[k] = str(v)

        try:
            body = self.raw_data.getvalue().encode()
        except AttributeError:
            body = self.raw_data.read()

        response = s3.Object(bucket, filename)\
                     .put(Body=body,
                          ContentType=self.content_type,
                          Metadata=metadata)

        logger.info(f"S3 upload response: {response}")

        return f"{bucket}/{filename}"

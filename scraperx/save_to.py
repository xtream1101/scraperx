import pathlib
import logging
from smart_open import open

from .utils import _get_s3_params, get_context_type

logger = logging.getLogger(__name__)


class SaveTo:

    def __init__(self, scraper, raw_data, content_type=None, encoding='utf-8'):
        self.scraper = scraper
        self.raw_data = raw_data
        self.content_type = content_type
        self.encoding = encoding

    def _get_filename(self, context=None, template_values={}, name_template=None):
        """Generate the filename based on the config template

        Args:
            context (class, optional): Either the Download or Extract class's self.
                Used to get timestamps and the correct template. Defaults to None.
            template_values (dict, optional): Additional keys to use in the template.
                Defaults to {}.
            name_template (str, optional): The config key for the template to use.
                If None is set then the `context` will be used.
                Defaults to None.

        Returns:
            str: The filename with the template values filled in
        """
        context_type = get_context_type(context)
        additional_args = {**self.scraper.log_extras()}
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
            name_template = self.scraper.config[f'{context_type}_FILE_TEMPLATE']

        task_safe = {}
        if context is not None:
            task_safe = {k: str(v) for k, v in context.task.items()}
        # Update so there cannot be duplicate keys, update order matters here
        additional_args.update(task_safe)
        additional_args.update(template_values)
        filename = name_template.format(**additional_args)

        return filename

    def save(self, context=None, template_values={}, filename=None, save_service=None):
        """Save the file based on the config

        Args:
            context (class, optional): Either the Download or Extract class's self.
                Used to get timestamps and the correct template. Defaults to None.
            template_values (dict, optional): Additional keys to use in the template.
                Defaults to {}.
            filename (str, optional): Use this as the filename. If None then the
                config file_template will be used. Defaults to None.
            save_service (str, optional): Override the service in the context.
                Defaults to None.

        Returns:
            str: File path to where it was saved
        """
        if save_service is None and context is not None:
            context_type = get_context_type(context)
            save_service = self.scraper.config[f'{context_type}_SAVE_DATA_SERVICE']

        filename = self._get_filename(context,
                                      template_values=template_values,
                                      name_template=filename)
        content_type = self.content_type
        if content_type is None:
            import mimetypes
            content_type, _ = mimetypes.guess_type(filename)

        if content_type is None:  # still....
            content_type = 'binary/octet-stream'

        if save_service == 's3':
            transport_params = _get_s3_params(self.scraper, context=context)
            transport_params['multipart_upload_kwargs'] = {'ContentType': content_type}
            if filename.startswith('s3://'):
                target_path = filename

            else:
                bucket_name = self.scraper.config[f'{context_type}_SAVE_DATA_BUCKET_NAME']
                filename = filename.replace('\\', '/')
                if not filename.startswith('/'):
                    filename = f"/{filename}"

                if filename.startswith('s3://'):
                    target_path = filename
                else:
                    target_path = f's3://{bucket_name}{filename}'

        else:
            target_path = filename
            # Create local directory if not exist
            pathlib.Path(target_path).parent.mkdir(parents=True, exist_ok=True)
            transport_params = {}

        try:
            with open(target_path, 'w',
                      transport_params=transport_params, encoding=self.encoding) as outfile:
                outfile.write(self.raw_data.read())

        except TypeError:
            self.raw_data.seek(0)
            # raw_data is BytesIO not StringIO
            with open(target_path, 'wb',
                      transport_params=transport_params, encoding=self.encoding) as outfile:
                outfile.write(self.raw_data.read())
        except AttributeError:
            # Data is bytes and does not need .read()
            with open(target_path, 'wb',
                      transport_params=transport_params, encoding=self.encoding) as outfile:
                outfile.write(self.raw_data)

        try:
            # Try and close if needed
            self.raw_data.close()
        except AttributeError:
            pass

        return target_path

import os
import logging
from smart_open import open

from .utils import _get_s3_params, get_context_type

logger = logging.getLogger(__name__)


class SaveTo:

    def __init__(self, scraper, raw_data, content_type=None):
        self.scraper = scraper
        self.raw_data = raw_data
        self.content_type = content_type

    def _get_filename(self, context, template_values={}, name_template=None):
        """Generate the filename based on the config template

        Args:
            context (class): Either the Download or Extract class's self.
                Used to get timestamps and the correct template.
            template_values (dict, optional): Additional keys to use in the template.
                Defaults to {}.
            name_template (str, optional): The config key for the template to use.
                If None is set then the `context` will be used.
                Defaults to None.

        Returns:
            str: The filename with the template values filled in
        """
        context_type = get_context_type(context)
        additional_args = {'scraper_name': self.scraper.config['SCRAPER_NAME']}
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

        task_safe = {k: str(v) for k, v in context.task.items()}
        filename = name_template.format(**task_safe,
                                        **template_values,
                                        **additional_args)

        return filename

    def save(self, context, template_values={}, filename=None):
        """Save the file based on the config

        Args:
            context (class): Either the Download or Extract class's self.
                Used to get timestamps and the correct template.
            template_values (dict, optional): Additional keys to use in the template.
                Defaults to {}.
            filename (str, optional): Use this as the filename. If None then the
                config file_template will be used. Defaults to None.

        Returns:
            str: File path to where it was saved
        """
        context_type = get_context_type(context)

        filename = self._get_filename(context,
                                      template_values=template_values,
                                      name_template=filename)
        content_type = self.content_type
        if content_type is None:
            import mimetypes
            content_type, _ = mimetypes.guess_type(filename)

        save_service = self.scraper.config[f'{context_type}_SAVE_DATA_SERVICE']
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
            transport_params = {}

        if '://' not in target_path:
            # Make sure dir is created
            try: os.makedirs(os.path.dirname(target_path))  # noqa: E701
            except OSError: pass  # noqa: E701

        try:
            with open(target_path, 'w', transport_params=transport_params) as outfile:
                outfile.write(self.raw_data.read())

        except TypeError:
            self.raw_data.seek(0)
            # raw_data is BytesIO not StringIO
            with open(target_path, 'wb', transport_params=transport_params) as outfile:
                outfile.write(self.raw_data.read())

        logger.debug(f"Saved file",
                     extra={'task': context.task,
                            'scraper_name': self.scraper.config['SCRAPER_NAME'],
                            'file': target_path})
        return target_path

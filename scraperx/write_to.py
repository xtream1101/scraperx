import io
import json
import logging
from .save_to import SaveTo

logger = logging.getLogger(__name__)


class WriteTo:

    def __init__(self, data):
        self.data = data

    def write(self):
        pass
        # TODO (will work like .save())

    def write_json(self):
        """Write json data to a StringIO object

        Returns:
            StringIO -- The data in a json format
        """
        json_args = {'sort_keys': True,
                     'indent': 4,
                     'ensure_ascii': False,
                     }

        if isinstance(self.data, str):
            # Convert raw string to dict to be saved as json
            json_data = json.loads(self.data)
        else:
            json_data = self.data

        output_io = io.StringIO()
        json.dump(json_data, output_io, **json_args)
        output_io.seek(0)
        return SaveTo(output_io, content_type='application/json')

    def write_file(self, content_type='text/html'):
        """Write data to a StringIO object without any additonal formatting

        Keyword Arguments:
            content_type {str} -- Used when saving the file
                                  (default: {'text/html'})

        Returns:
            StringIO -- The data
        """
        output_io = io.StringIO()
        output_io.write(self.data)
        output_io.seek(0)
        return SaveTo(output_io, content_type=content_type)

    def write_zip(self, content_type='application/zip'):
        """Write data to a StringIO object without any additonal formatting

        Keyword Arguments:
            content_type {str} -- Used when saving the file
                                  (default: {'text/html'})

        Returns:
            StringIO -- The data
        """
        output_io = io.BytesIO()
        output_io.write(self.data)
        output_io.seek(0)
        return SaveTo(output_io, content_type=content_type)

    def write_csv(self, filename):
        # TODO
        raise NotImplementedError

    def write_xlsx(self, filename):
        # TODO
        raise NotImplementedError

    def write_parquet(self, filename):
        # TODO
        raise NotImplementedError

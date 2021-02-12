import io
import json
import logging
from .save_to import SaveTo

logger = logging.getLogger(__name__)


class Write:

    def __init__(self, scraper, data, encoding='utf-8'):
        self.scraper = scraper
        self.data = data
        self.encoding = encoding

    def write(self):
        # TODO: (will work like .save())
        raise NotImplementedError

    def write_json(self, json_args=None):
        """Write json data to a StringIO object

        Returns:
            class: scraper.save_to.SaveTo, Used to then save the file
        """
        if json_args is None:
            json_args = {'sort_keys': True,
                         'indent': 4,
                         'ensure_ascii': False,
                         }

        if isinstance(self.data, str):
            # Convert raw string into a list to be saved as json
            json_data = [self.data]
        else:
            json_data = self.data

        # TODO: Convert all non json types into strings.

        output_io = io.StringIO()
        json.dump(json_data, output_io, **json_args)
        output_io.seek(0)
        return SaveTo(self.scraper, output_io,
                      content_type='application/json',
                      encoding=self.encoding)

    def write_json_lines(self, json_args=None):
        """Write json data to a StringIO object

        Returns:
            class: scraper.save_to.SaveTo, Used to then save the file
        """
        if json_args is None:
            json_args = {'sort_keys': True,
                         'ensure_ascii': False,
                         }

        if not isinstance(self.data, (list, tuple)):
            # Convert raw string into a list to be saved as json
            json_data = [self.data]
        else:
            json_data = self.data

        # TODO: Convert all non json types into strings.

        output_io = io.StringIO()
        for row in json_data:
            json.dump(row, output_io, **json_args)
            output_io.write('\n')

        output_io.seek(0)
        return SaveTo(self.scraper, output_io,
                      content_type='application/json',
                      encoding=self.encoding)

    def write_file(self, content_type=None):
        """Write data to a StringIO/BytesIO object without any additional formatting

        Args:
            content_type (str): Used when saving the file. Defaults to None.

        Returns:
            class: scraper.save_to.SaveTo, Used to then save the file
        """
        try:
            output_io = io.StringIO()
            output_io.write(self.data)
            output_io.seek(0)
        except TypeError:
            output_io = io.BytesIO()
            output_io.write(self.data)
            output_io.seek(0)

        return SaveTo(self.scraper, output_io,
                      content_type=content_type,
                      encoding=self.encoding)

    def write_csv(self, filename):
        # TODO
        raise NotImplementedError

    def write_xlsx(self, filename):
        # TODO
        raise NotImplementedError

    def write_parquet(self):
        # TODO: Test and create docs for
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Generate the schema
        field_list = []
        # TODO: Catch and exit gracefully
        # Will fail if the extractor does not have the class var schema_fields
        for k, v in self.schema_fields.items():
            field_list.append(pa.field(k, v))

        schema = pa.schema(field_list)

        # Create pyarrow table
        column_names = []
        columns = []
        for column in schema:
            column_values = [dic.get(column.name) for dic in self.data]
            try:
                columns.append(pa.array(column_values, type=column.type))
            except Exception:
                logger.exception(("Could not create array"
                                  f" for column: {column.name}"))
                raise
            column_names.append(column.name)

        record_batch = pa.RecordBatch.from_arrays(columns, column_names)
        table = pa.Table.from_batches([record_batch])

        output_io = io.BytesIO()
        pq.write_table(table, output_io)
        output_io.seek(0)

        # TODO: Is this the correct content type for a parquet file?
        return SaveTo(self.scraper, output_io,
                      content_type='application/octet-stream',
                      encoding=self.encoding)

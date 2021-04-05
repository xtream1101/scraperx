import os
import json
import logging
import pathlib
import unittest
from pprint import pprint
from deepdiff import DeepDiff

from .utils import read_file_contents

logger = logging.getLogger(__name__)


def _clean_keys(i_keys, prev_dict):
    if not isinstance(i_keys, (tuple, list)):
        i_keys = [i_keys]

    for i_key in i_keys:
        if isinstance(i_key, str):
            prev_dict.update({i_key: None})
        else:
            for key, more_i_keys in i_key.items():
                prev_dict.update({key: _clean_keys(more_i_keys, {})})

    return prev_dict


class ExtractorBaseTest:
    # Wrap actual test case in a blank class so it's not run on its own
    #   (only when called through derived class)
    # From https://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class

    class TestCase(unittest.TestCase):

        def __init__(self, sample_data_dir, scraper, ignore_keys=None, *args, **kwargs):
            """Test QA'd extracted data against the current extractor code

            `ignore_keys` usage::
                test_data = [{'foo': 'bar0',
                            'a': 1,
                            'b': {'1': 'one',
                                    '2': 'two',
                                    },
                            },
                            {'foo': 'bar1',
                            'a': 1,
                            'b': {'1': 'one again',
                                    '2': 'two once more',
                                    },
                            },
                            ]
            To ignore the value that is set in the keys::
                `test_data['foo']` and in `test_data['b']['2']`
                `ignore_keys = ['foo', {'b': ['2']}]`

            Args:
                sample_data_dir (str): Path to the sample data for the scraper.
                    Relative from the root of the repo
                scraper (scraperx.Scraper): The scrapers scraper class
                ignore_keys (list, optional): A list of keys to ignore.
                    Useful when dealing with timestamps that change
                    on each re-extract. Default is None.
            """
            super().__init__(*args, **kwargs)
            self.directory = os.fsencode(sample_data_dir)
            self.scraper = scraper
            self.scraper.config._set_value('SCRAPER_NAME', sample_data_dir.split('/')[2])
            if ignore_keys is None:
                ignore_keys = []
            self._ignore_keys = _clean_keys(ignore_keys, {})
            self.metadata_files = []

        def setUp(self):
            for file in os.listdir(self.directory):
                fname = os.fsdecode(file)
                if fname.endswith('metadata.json'):
                    self.metadata_files.append(
                        os.path.join(
                            self.directory.decode("utf-8"),
                            fname
                        )
                    )

        def _compare_data(self, test_data, qa_file):
            qa_data = json.loads(read_file_contents(qa_file))

            for row in qa_data:
                row.update(self._ignore_keys)
            for row in test_data:
                row.update(self._ignore_keys)
            diff = DeepDiff(qa_data, test_data)
            if diff:
                pprint(diff)
                errors = json.dumps(diff,
                                    sort_keys=True,
                                    indent=4,
                                    ensure_ascii=False)
                # Fail the test with the things that changed
                with self.subTest(qa_file):
                    self.assertEqual(diff, {}, '\n' + errors)

        def _test_source_file(self, extractor, s_idx, s_file, metadata):
            raw_source = read_file_contents(s_file)

            time_downloaded = (metadata['download_manifest']['time_downloaded']
                               .replace('-', '').replace(':', ''))
            dst_base = pathlib.Path(
                f"tests/sample_data/{self.scraper.config['SCRAPER_NAME']}/{time_downloaded}")

            def _tester_format_extract_task(inputs):
                inputs = extractor.original_format_extract_task(inputs)
                inputs['post_extract'] = self._compare_data
                qa_file = f"{dst_base}_extracted_qa_{inputs['name']}_{s_idx}.json"
                inputs['post_extract_kwargs'] = {'qa_file': qa_file}
                return inputs
            extractor._format_extract_task = _tester_format_extract_task

            for e_task in extractor._get_extraction_tasks(raw_source, s_idx):
                e_task(raw_source)

        def runTest(self):  # noqa: N802
            for metadata_file in self.metadata_files:
                with pathlib.Path(metadata_file).open(mode='r') as f:
                    metadata = json.load(f)

                extractor = self.scraper.extract(metadata['task'],
                                                 metadata['download_manifest'])
                # Override post_extract values to force it to save locally in a json format
                extractor.original_format_extract_task = extractor._format_extract_task
                metadata_sources = metadata['download_manifest']['source_files']
                for source_idx, source in enumerate(metadata_sources):
                    source_file = pathlib.Path(source['file'])
                    self._test_source_file(extractor,
                                           source_idx,
                                           source_file,
                                           metadata)

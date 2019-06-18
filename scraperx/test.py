import os
import json
import logging
import unittest
from pprint import pprint
from deepdiff import DeepDiff

from .config import config

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

        def __init__(self, sample_data_dir, extract_cls, ignore_keys=None, *args, **kwargs):
            """Test QA'd extracted data against the current extractor code

            `ignore_keys` usage:
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
            To ignore the value that is set in the keys
            `test_data['foo']` and in `test_data['b']['2']`
            `ignore_keys = ['foo', {'b': ['2']}]`

            Arguments:
                sample_data_dir {str} -- Path to the sample data for the scraper.
                                         Relative from the root of the repo
                extract_cls {objet} -- Extract class of the scraper

            Keyword Arguments:
                ignore_keys {list} -- A list of keys to ignore.
                                      Useful when dealing with timestamps that change
                                      on each re-extract (default: {None})
            """
            super().__init__(*args, **kwargs)
            self.directory = os.fsencode(sample_data_dir)
            config._set_value('SCRAPER_NAME', sample_data_dir.split(os.path.sep)[2])
            self.extract_cls = extract_cls
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
            with open(qa_file, 'r') as f:
                qa_data = json.load(f)

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
            with open(s_file, 'r') as f:
                raw_source = f.read()

            dst_base = os.path.join(
                'tests',
                'sample_data',
                config['SCRAPER_NAME'],
                metadata['download_manifest']['time_downloaded']
                .replace('-', '').replace(':', ''),
            )

            e_tasks = extractor._get_extraction_tasks(raw_source, s_idx)
            for e_task in e_tasks:
                # Check if there si a qa file for this source
                qa_file = f"{dst_base}_extracted_qa_{e_task['name']}_{s_idx}.json"  # noqa: E501
                if not os.path.isfile(qa_file):
                    logger.warning(f"QA file not found: {qa_file}",
                                   extra={'task': metadata['task'],
                                          'scraper_name': config['SCRAPER_NAME']})  # noqa: E501
                    continue
                # Override what happens with the extracted data
                e_task['post_extract'] = self._compare_data
                e_task['post_extract_kwargs'] = {'qa_file': qa_file}
                extractor._extraction_task(e_task, raw_source)

        def runTest(self):  # noqa: N802
            for metadata_file in self.metadata_files:
                print(metadata_file)
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)

                extractor = self.extract_cls(metadata['task'],
                                             metadata['download_manifest'])
                metadata_sources = metadata['download_manifest']['source_files']
                for source_idx, source in enumerate(metadata_sources):
                    self._test_source_file(extractor,
                                           source_idx,
                                           source['file'],
                                           metadata)

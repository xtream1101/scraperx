import json
import pytest
import pathlib
from moto import mock_s3

from scraperx import utils


def test_get_encoding_local():
    """Confirm that the test file is the encoding we expect it to be
    """
    guessed_encoding = utils.get_encoding(
        pathlib.Path('./tests/files/windows_1252_encoded.json').read_bytes()
    )
    assert guessed_encoding == 'WINDOWS-1252'


@mock_s3
def test_read_file_contents_s3():
    import boto3
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket & add test file since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='test-bucket')
    s3.upload_file('./tests/files/windows_1252_encoded.json',
                   'test-bucket',
                   'test_file.json')
    raw_data = json.loads(utils.read_file_contents('s3://test-bucket/test_file.json'))
    assert raw_data[1]['title'] == 'No. 4 Bond Maintenance™ Shampoo'


def test_file_is_not_utf8():
    """Test that our example file cannot be read in using utf8
    """
    with pytest.raises(UnicodeDecodeError):
        with open('./tests/files/windows_1252_encoded.json', 'r') as f:
            f.read()


def test_read_file_contents_local():
    """Test that the non-utf8 char is read into the json object correctly
    """
    raw_data = json.loads(utils.read_file_contents('./tests/files/windows_1252_encoded.json'))
    assert raw_data[1]['title'] == 'No. 4 Bond Maintenance™ Shampoo'


def test_rate_limit_from_period():
    qps = utils.rate_limit_from_period(3600, 1)
    assert qps == 1


def test_rate_limit_fn():
    import time

    @utils.rate_limited(num_calls=5, every=1)
    def run_fn():
        return time.time()

    # Run the function 4 times
    times = []
    for _ in range(4):
        times.append(run_fn())

    # Calc the average diff in each timestamp
    diffs = [x - times[i - 1] for i, x in enumerate(times)][1:]
    avg_diff = sum(diffs) / len(diffs)

    # Is average diff within an error of margin?
    assert avg_diff > 0.19 and avg_diff < 0.21

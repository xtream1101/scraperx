import os
import sys
import json
import logging
from pprint import pprint
from deepdiff import DeepDiff

from .write import Write
from .config import config

logger = logging.getLogger(__name__)


def _run_test(cli_args, extract_cls):
    # TODO: Loop over test dir for scraper and extract
    #       compare the json files using code from centrifuge
    #       delete the test file after the check runs
    for _ in ['file']:  # TODO: thread for all files
        task = {
            "device_type": "desktop",
            "id": 0,
            "scraper_name": config['SCRAPER_NAME'],
            "url": "http://testing-ground.scraping.pro/blocks"
        }
        source_file = f"test_sources/{config['SCRAPER_NAME']}/test_1.html"
        download_manifest = {
            "date_downloaded": "2019-01-21",
            "source_files": [
                {
                    "location": "local",
                    "path": source_file
                }
            ],
            "time_downloaded": "2019-01-21 21:27:36.485610",
        }
        # TODO: Forcing this will fail for scrapers that extract
        #       to multiple files. Need to fix
        config._set_value('EXTRACTOR_FILE_TEMPLATE',
                          f"{source_file}_test.json")

        extractor = extract_cls(task, download_manifest)
        test_files = extractor.run()

        # TODO: Make work for multiple extracted files
        # for test_file in test_files:

        # Compare the qa files for each output
        data = {}
        test_file = test_files[0]['path']
        with open(test_file) as test_data:
            data['new'] = json.load(test_data)

        qa_file = f"{source_file}_qa.json"
        with open(qa_file) as qa_data:
            data['qa'] = json.load(qa_data)

        diff = DeepDiff(data['qa'], data['new'])
        if diff != {}:
            logger.error(f"Errors found in the file {qa_file}",
                         extra={'task': task,
                                'scraper_name': config['SCRAPER_NAME']})
            pprint(diff)
        else:
            logger.info("All test qa'd files passed the extraction test",
                        extra={'task': task,
                               'scraper_name': config['SCRAPER_NAME']})

        os.remove(test_file)


def _run_dispatch(cli_args, dispatch_cls, download_cls, extract_cls):
    """Kick off the dispatcher for the scraper

    Arguments:
        dispatch_cls {class} -- Class of the dispatch action from the scraper
    """
    tasks = None
    if cli_args.tasks:
        tasks = cli_args.tasks

    dispatcher = dispatch_cls(tasks=tasks,
                              download_cls=download_cls,
                              extract_cls=extract_cls)
    if cli_args.dump_tasks:
        # Dump data to local json file
        task_file = Write(dispatcher.tasks).write_json()\
                                           .save_local('tasks.json')
        num_tasks = len(dispatcher.tasks)
        logger.info(f"Saved {num_tasks} tasks to {task_file['path']}",
                    extra={'task': None,
                           'scraper_name': config['SCRAPER_NAME']})

    # Run the dispatcher...
    dispatcher.dispatch()


def _run_download(cli_args, download_cls, extract_cls):
    """Kick off the downloader for the scraper

    Arguments:
        download_cls {class} -- Class of the download action from the scraper
    """
    for task in cli_args.tasks:
        downloader = download_cls(task, extract_cls=extract_cls)
        downloader.run()


def _run_extract(cli_args, extract_cls):
    """Kick off the extractor for the scraper

    Arguments:
        extract_cls {class} -- Class of the extract action from the scraper
    """
    if os.path.isdir(cli_args.source):
        metadata_files = []
        for file in os.listdir(os.fsencode(cli_args.source)):
            filename = os.fsdecode(file)
            if filename.endswith('.metadata.json'):
                metadata_files.append(os.path.join(cli_args.source, filename))
    else:
        metadata_files = [f"{cli_args.source}.metadata.json"]

    for metadata_file in metadata_files:
        metadata = {}
        with open(metadata_file) as f:
            metadata = json.load(f)

        extractor = extract_cls(metadata['task'],
                                metadata['download_manifest'])
        extractor.run()


def run_cli(dispatch_cls=None, download_cls=None, extract_cls=None):
    from .arguments import cli_args
    config_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])),
                               'config.yaml')
    config.load_config(config_file, cli_args=cli_args)
    if cli_args.action == 'validate':
        from pprint import pprint
        logger.info("Testing the config....",
                    extra={'task': None,
                           'scraper_name': config['SCRAPER_NAME']})
        pprint(config.values)

    elif cli_args.action == 'test':
        _run_test(cli_args, extract_cls)

    elif cli_args.action == 'dispatch':
        _run_dispatch(cli_args, dispatch_cls, download_cls, extract_cls)

    elif cli_args.action == 'download':
        _run_download(cli_args, download_cls, extract_cls)

    elif cli_args.action == 'extract':
        _run_extract(cli_args, extract_cls)

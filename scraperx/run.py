import os
import json
import logging
from .write_to import WriteTo
from .arguments import cli_args
from .config import config


logger = logging.getLogger(__name__)


def run_dispatch(dispatch_cls):
    """Kick off the dispatcher for the scraper

    Arguments:
        dispatch_cls {class} -- Class of the dispatch action from the scraper
    """
    tasks = None
    if cli_args.tasks:
        tasks = cli_args.tasks

    # If a limit is set, the dispatcher init will handle it
    dispatcher = dispatch_cls(tasks=tasks)
    if cli_args.dump_tasks:
        # Dump data to local json file
        task_file = WriteTo(dispatcher.tasks).write_json()\
                                             .save_local('tasks.json')
        num_tasks = len(dispatcher.tasks)
        logger.info(f"Saved {num_tasks} tasks to {task_file['path']}")

    # Run the dispatcher...
    dispatcher.dispatch()


def run_download(download_cls):
    """Kick off the downloader for the scraper

    Arguments:
        download_cls {class} -- Class of the download action from the scraper
    """
    for task in cli_args.tasks:
        downloader = download_cls(task)
        downloader.run()


def run_extract(extract_cls):
    """Kick off the extractor for the scraper

    Arguments:
        extract_cls {class} -- Class of the extract action from the scraper
    """
    if os.path.isdir(cli_args.source):
        # source_dir = os.fsencode(cli_args.source)
        # for file in os.listdir(source_dir):

        # extractor = cli_args.scraper.Extract(task, cli_args=args)
        # extractor.run()
        pass
    else:
        metadata_file = f"{cli_args.source}.metadata.json"
        metadata = {}
        with open(metadata_file) as f:
            metadata = json.load(f)

        extractor = extract_cls(metadata['task'],
                                metadata['download_manifest'])
        extractor.run()


def run(dispatch=None, download=None, extract=None):

    print("Run:", cli_args.action)
    if cli_args.action == 'validate':
        from pprint import pprint
        print("Testing the config....")
        pprint(config.values)
    elif cli_args.action == 'dispatch':
        run_dispatch(dispatch)
    elif cli_args.action == 'download':
        run_download(download)
    elif cli_args.action == 'extract':
        run_extract(extract)

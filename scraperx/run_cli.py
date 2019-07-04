import os
import sys
import json
import logging
from shutil import copyfile

from .write import Write
from .config import config

logger = logging.getLogger(__name__)


def _create_test(cli_args, extract_cls):
    # TODO: Support pulling files from s3
    # Move the metadata file and all sources into the test sample data dir
    # Update the metadata file to include the new source paths
    #

    metadata_file = cli_args.metadata
    if not metadata_file.endswith('_metadata.json'):
        logger.critical("Input file must be the source metadata file",
                        extra={'task': None,
                               'scraper_name': config['SCRAPER_NAME']})
        sys.exit(1)

    with open(metadata_file, 'r') as f:
        metadata = json.load(f)

    dst_base = os.path.join('tests',
                            'sample_data',
                            config['SCRAPER_NAME'],
                            metadata['download_manifest']['time_downloaded']
                            .replace('-', '').replace(':', ''),
                            )

    # make sure dir esists
    try:
        os.makedirs(os.path.dirname(dst_base))
    except OSError:
        pass

    current_sources = metadata['download_manifest']['source_files'].copy()
    metadata_sources = []
    for idx, source in enumerate(current_sources):
        new_file = f"{dst_base}_source_{idx}.{source['file'].split('.')[-1]}"
        copyfile(source['file'], new_file)
        source['file'] = new_file
        metadata_sources.append(source)

    metadata['download_manifest']['source_files'] = metadata_sources

    # Save metadata
    with open(f'{dst_base}_metadata.json', 'w') as f:
        json.dump(metadata, f,
                  sort_keys=True,
                  indent=4,
                  ensure_ascii=False)

    # Run the extractor
    def save_extracted(data, source_idx, name):
        data_name = f'{dst_base}_extracted_(qa)_{name}_{source_idx}.json'
        Write(data).write_json().save_local(data_name)

    extractor = extract_cls(metadata['task'],
                            metadata['download_manifest'])
    for source_idx, source in enumerate(metadata_sources):
        raw_source = None
        with open(source['file'], 'r') as f:
            raw_source = f.read()
        e_tasks = extractor._get_extraction_tasks(raw_source, source_idx)
        for e_task in e_tasks:
            # Override what happens with the extracted data
            e_task['post_extract'] = save_extracted
            e_task['post_extract_kwargs'] = {'source_idx': source_idx,
                                             'name': e_task['name'],
                                             }
            extractor._extraction_task(e_task, raw_source)

    logger.info((f"Test files created under {dst_base}*."
                 f" Please QA the extracted files"),
                extra={'task': metadata['task'],
                       'scraper_name': config['SCRAPER_NAME']})


def _run_dispatch(cli_args, dispatch_cls, download_cls, extract_cls):
    """Kick off the dispatcher for the scraper

    Arguments:
        dispatch_cls {class} -- Class of the dispatch action from the scraper
    """
    tasks = None
    if cli_args.tasks:
        tasks = cli_args.tasks

    def dump_tasks(tasks):
        # Dump all tasks to local json file
        task_file = Write(tasks).write_json().save_local('tasks.json')
        num_tasks = len(tasks)
        logger.info(f"Saved {num_tasks} tasks to {task_file}",
                    extra={'task': None,
                           'scraper_name': config['SCRAPER_NAME']})

    dispatcher = dispatch_cls(tasks=tasks,
                              download_cls=download_cls,
                              extract_cls=extract_cls)
    try_dump_after = False
    if cli_args.dump_tasks:
        if dispatcher.tasks:
            dump_tasks(dispatcher.tasks)
        else:
            # there are either no tasks or the scraper returned a generator
            # in which case the tasks are not known until after they have all
            # been dispatched
            try_dump_after = True

    # Run the dispatcher...
    dispatcher.dispatch()

    if cli_args.dump_tasks and try_dump_after is True:
        dump_tasks(dispatcher.tasks)


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
        for root, dirs, files in os.walk(cli_args.source):
            for filename in files:
                if filename.endswith('_metadata.json'):
                    metadata_files.append(os.path.join(root, filename))
    else:
        if cli_args.source.endswith('_metadata.json'):
            metadata_files = [cli_args.source]
        else:
            metadata_files = [f"{cli_args.source}_metadata.json"]

    for metadata_file in metadata_files:
        metadata = {}
        with open(metadata_file) as f:
            metadata = json.load(f)

        extractor = extract_cls(metadata['task'],
                                metadata['download_manifest'])
        extractor.run()


def run_cli(dispatch_cls=None, download_cls=None, extract_cls=None):
    from .arguments import cli_args
    config.load_config(cli_args=cli_args)
    if cli_args.action == 'validate':
        from pprint import pprint
        logger.info("Testing the config....",
                    extra={'task': None,
                           'scraper_name': config['SCRAPER_NAME']})
        pprint(config.values)

    elif cli_args.action == 'create-test':
        _create_test(cli_args, extract_cls)

    elif cli_args.action == 'dispatch':
        _run_dispatch(cli_args, dispatch_cls, download_cls, extract_cls)

    elif cli_args.action == 'download':
        _run_download(cli_args, download_cls, extract_cls)

    elif cli_args.action == 'extract':
        _run_extract(cli_args, extract_cls)

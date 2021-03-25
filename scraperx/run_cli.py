import os
import sys
import json
import shutil
import logging
import pathlib

from .write import Write
from .utils import read_file_contents

logger = logging.getLogger(__name__)


def _create_test(cli_args, scraper):
    # TODO: Support pulling files from s3
    # Move the metadata file and all sources into the test sample data dir
    # Update the metadata file to include the new source paths

    metadata_file = cli_args.metadata
    if not metadata_file.endswith('_metadata.json'):
        logger.critical("Input file must be the source metadata file",
                        extra={'scraper_name': scraper.config['SCRAPER_NAME']})
        sys.exit(1)

    with pathlib.Path(metadata_file).open(mode='r') as f:
        metadata = json.load(f)

    time_downloaded_cleaned = (metadata['download_manifest']['time_downloaded']
                               .replace('-', '').replace(':', ''))
    dst_base = pathlib.Path(
        f"tests/sample_data/{scraper.config['SCRAPER_NAME']}/{time_downloaded_cleaned}")

    # make sure dir exists
    dst_base.parent.mkdir(parents=True, exist_ok=True)

    current_sources = metadata['download_manifest']['source_files'].copy()
    metadata_sources = []
    for idx, source in enumerate(current_sources):
        new_file = pathlib.Path(f"{dst_base}_source_{idx}.{source['file'].split('.')[-1]}")
        shutil.copy(source['file'], new_file)
        source['file'] = new_file.as_posix()
        metadata_sources.append(source)

    metadata['download_manifest']['source_files'] = metadata_sources

    # Save metadata
    with pathlib.Path(f'{dst_base}_metadata.json').open(mode='w') as f:
        json.dump(metadata, f,
                  sort_keys=True,
                  indent=4,
                  ensure_ascii=False)

    extractor = scraper.extract(metadata['task'],
                                metadata['download_manifest'])
    # Override post_extract values to force it to save locally in a json format
    extractor.original_format_extract_task = extractor._format_extract_task

    def save_extracted(data, source_idx, name):
        data_name = f'{dst_base}_extracted_(qa)_{name}_{source_idx}.json'
        Write(scraper, data).write_json().save(extractor, filename=data_name)

    def _tester_format_extract_task(inputs):
        inputs = extractor.original_format_extract_task(inputs)
        inputs['post_extract'] = save_extracted
        inputs['post_extract_kwargs'] = {'source_idx': source_idx,
                                         'name': inputs['name'],
                                         }
        return inputs
    extractor._format_extract_task = _tester_format_extract_task

    for source_idx, source in enumerate(metadata_sources):
        raw_source = read_file_contents(source['file'])

        for e_task in extractor._get_extraction_tasks(raw_source, source_idx):
            e_task(raw_source)

    logger.info((f"Test files created under {dst_base}_*."
                 f" Please QA the extracted files"),
                extra={'task': metadata['task'],
                       'scraper_name': scraper.config['SCRAPER_NAME']})


def _run_dispatch(cli_args, scraper):
    """Kick off the dispatcher for the scraper
    """
    tasks = None
    if cli_args.tasks:
        tasks = cli_args.tasks

    def dump_tasks(tasks):
        # Dump all tasks to local json file
        task_file = Write(scraper, tasks).write_json().save(None, filename='tasks.json')
        num_tasks = len(tasks)
        logger.info(f"Saved {num_tasks} tasks to {task_file}",
                    extra={'scraper_name': scraper.config['SCRAPER_NAME']})

    dispatcher = scraper.dispatch(tasks=tasks)
    try_dump_after = False
    if cli_args.dump_tasks:
        try:
            dump_tasks(dispatcher.tasks)
        except AttributeError:
            # Scraper returned a generator
            # in which case the tasks are not known until after they have all
            # been dispatched
            try_dump_after = True

    # Run the dispatcher...
    dispatcher.run()

    if cli_args.dump_tasks and try_dump_after is True:
        dump_tasks(dispatcher.tasks)


def _run_download(cli_args, scraper):
    """Kick off the downloader for the scraper
    """
    for task in cli_args.tasks:
        downloader = scraper.download(task)
        downloader.run()


def _run_extract(cli_args, scraper):
    """Kick off the extractor for the scraper
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

        extractor = scraper.extract(metadata['task'],
                                    metadata['download_manifest'])
        extractor.run()


def run_cli(scraper):
    """Called by the user when running the scraper

    This should be called in the scrapers __main__ like so::

        scraper = Scraper(dispatch_cls=MyDispatch,
                          download_cls=MyDownload,
                          extract_cls=MyExtract)
        if __name__ == '__main__':
            run_cli(scraper)

    Args:
        scraper (class): The scraperx.Scraper class that was setup in the users scraper
    """
    from .arguments import parser

    cli_args = parser.parse_known_args()[0]
    scraper.config.load_config(cli_args=cli_args)
    if cli_args.action == 'validate':
        from pprint import pprint
        logger.info("Testing the config....",
                    extra={**scraper.log_extras()})
        pprint(scraper.config.values)

    elif cli_args.action == 'create-test':
        _create_test(cli_args, scraper)

    elif cli_args.action == 'dispatch':
        _run_dispatch(cli_args, scraper)

    elif cli_args.action == 'download':
        _run_download(cli_args, scraper)

    elif cli_args.action == 'extract':
        _run_extract(cli_args, scraper)

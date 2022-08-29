# -*- coding: utf-8 -*-
import logging

import click as click

from src.parse_sap_file.file_analizer import FileAnalyzer
from src.parse_sap_file.get_header_mapping import HeaderMapping
from src.parse_sap_file.parse_manager import ParseFileManager

logging.getLogger("charset_normalizer").setLevel(logging.CRITICAL)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


@click.command()
@click.option('--source_path', '-s', type=click.Path(exists=True),
              help="Full, valid path to source sap(pipe delimited, prefixed/suffixed file")
@click.option('--analyze', '-a', required=False, is_flag=True, default=False,
              help="Analyze file.")
@click.option('--columns_count', '-c', required=False, type=click.IntRange(min=1, max=None),
              help="Expected columns count in _rows with pipe prefix/suffix")
@click.option('--target_file_path', '-t', required=False, default='', type=click.Path(exists=False),
              help="If specific output path needed. Default will generate output csv from source path")
def cli(source_path, analyze, columns_count, target_file_path):

    if not source_path:
        click.echo('Try --help for help')
        exit()

    file_info = FileAnalyzer(file_path=source_path, expected_columns_count=columns_count)

    if analyze:
        print(file_info.guess_columns_count())
        exit()

    file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                            columns_count=file_info.expected_columns_count)

    parser = ParseFileManager(source_path, target_file_path, file_analyzer=file_info)
    parser.pars()


if __name__ == '__main__':
    cli()

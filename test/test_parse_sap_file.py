import os

import pytest

from helpers.helper import timeit_function
from src.parse_sap_file.file_analizer import FileAnalyzer
from src.parse_sap_file.get_header_mapping import HeaderMapping
from src.parse_sap_file.parse_manager import handle_extra_delimiter, ParseFileManager


class TestParseSapFile:

    def test_file_parser_sap_file_1_encoding_ascii(self, get_file_parser_sap_file_1, sap_file_1):
        p = get_file_parser_sap_file_1
        with timeit_function('parse time'):
            result = p.get_file_encoding(sap_file_1.path)
        assert result == 'utf_8'

    def test_file_parser_sap_file_1_file_head_rows_true(self, get_file_parser_sap_file_1, sap_file_1_head_body):
        file_info = get_file_parser_sap_file_1
        with timeit_function('parse time'):
            assert file_info.table_header_text == sap_file_1_head_body[15].strip()

    def test_file_analyzer_sap_file_1_file_header_mapping(self, sap_file_1, sap_file_1_columns_header_mapping):
        file_info = FileAnalyzer(file_path=sap_file_1.path, expected_columns_count=sap_file_1.columns_count)
        file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                                columns_count=file_info.expected_columns_count)
        assert file_info._column_header_mapping == sap_file_1_columns_header_mapping

    def test_file_parser_sap_file_1_file_header_text(self, sap_file_1, sap_file_1_table_header_text):
        file_info = FileAnalyzer(file_path=sap_file_1.path, expected_columns_count=sap_file_1.columns_count)
        file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                                columns_count=file_info.expected_columns_count)

        assert file_info._table_header_text == sap_file_1_table_header_text

    def test_run_read_small_file(self, sap_file_1, test_data_folder):

        file_info = FileAnalyzer(file_path=sap_file_1.path, expected_columns_count=sap_file_1.columns_count)
        file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                                columns_count=file_info.expected_columns_count)
        parser = ParseFileManager(sap_file_1.path, file_analyzer=file_info)
        parser.pars()
        assert parser.lines_counter == 78

    # @pytest.mark.skip(reason="no way of currently testing this")
    def test_run_read_largefile(self, get_path_sap_file_2_lager):
        file_info = FileAnalyzer(file_path=get_path_sap_file_2_lager, expected_columns_count=24)
        file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                                columns_count=file_info.expected_columns_count)
        p = ParseFileManager(get_path_sap_file_2_lager, file_analyzer=file_info)
        with timeit_function(f'parse {get_path_sap_file_2_lager}'):
            p.pars()

        assert p.lines_counter == 460872

    # @pytest.mark.skip(reason="no way of currently testing this")
    def test_run_read_1gbfile(self, get_path_sap_file_3_1gb_file):
        file_info = FileAnalyzer(file_path=get_path_sap_file_3_1gb_file, expected_columns_count=17)
        file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                                columns_count=file_info.expected_columns_count)
        p = ParseFileManager(get_path_sap_file_3_1gb_file, file_analyzer=file_info)
        with timeit_function(f'parse {get_path_sap_file_3_1gb_file}'):
            p.pars()

        assert p.lines_counter == 4500011

    def test_run_read_mega_file(self, get_path_sap_file_4_mega_file):
        file_info = FileAnalyzer(file_path=get_path_sap_file_4_mega_file, expected_columns_count=17)
        file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                                columns_count=file_info.expected_columns_count)
        p = ParseFileManager(get_path_sap_file_4_mega_file, file_analyzer=file_info)
        with timeit_function(f'parse {get_path_sap_file_4_mega_file}'):
            p.pars()

        assert p.lines_counter == 7411025

    @pytest.mark.skip(reason="no way of currently testing this")
    def test_run_read_encoding(self, test_data_encoding_folder):
        encoding_results: list = list()
        print()
        for root, dirs, files in os.walk(test_data_encoding_folder):
            for f in files:
                file_path = os.path.join(root, f)
                file_analyzer = FileAnalyzer(file_path=file_path, expected_columns_count=30)
                out = (f, file_analyzer.file_encoding)
                encoding_results.append(out)

        assert encoding_results == [('die_ISO-8859-1.txt', 'latin_1'),
                                    ('file_guide.csv', 'utf_8'),
                                    ('harpers_ASCII.txt', 'ascii'),
                                    ('olaf_Windows-1251.txt', 'cp1251'),
                                    ('portugal_ISO-8859-1.txt', 'latin_1'),
                                    ('sap_file_1.csv', 'ascii'),
                                    ('shisei_UTF-8.txt', 'utf_8'),
                                    ('yan_BIG-5.txt', 'big5')]

    def test_if_columns_len_can_mach(self, get_path_header_exception_file):
        with open(get_path_header_exception_file, 'r') as f:
            sap_1p = f.readlines()

        header_columns_mapping = [int(_) for _ in sap_1p[2].split(',')]
        lines = sap_1p[3:]
        fix: list = []
        line: str = ''
        for line in lines:
            fix = handle_extra_delimiter(line.split('|')[1:-1], header_columns_mapping, '|')
        assert '|' + '|'.join(fix) + '|' == line.rstrip()

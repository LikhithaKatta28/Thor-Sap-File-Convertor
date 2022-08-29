# -*- coding: utf-8 -*-
from dataclasses import dataclass
import os

import pytest

from src.parse_sap_file.file_analizer import FileAnalyzer
from src.parse_sap_file.get_header_mapping import HeaderMapping
from src.parse_sap_file.parse_manager import ParseFileManager


@pytest.fixture(scope="module")
def test_data_folder():
    return os.path.join(os.path.dirname(__file__), 'test_data')


@pytest.fixture(scope="module")
def test_data_encoding_folder():
    return os.path.join(os.path.dirname(__file__), 'test_data_encoding')


@pytest.fixture(scope="module")
def sap_file_1(test_data_folder):
    @dataclass
    class SapFile1:
        path = os.path.join(test_data_folder, 'sap_file_1.txt')
        columns_count = 30

    return SapFile1


@pytest.fixture(scope="module")
def sap_file_1_head_body(test_data_folder):
    with open(os.path.join(test_data_folder, 'sap_file_1_head.txt'), 'r') as f:
        return f.readlines()


@pytest.fixture(scope="module")
def sap_file_1_columns_header_mapping(test_data_folder):
    return [15, 16, 6, 6, 9, 40, 22, 10, 10, 20, 6, 15, 18, 40, 4, 9, 3, 10, 24, 26, 4, 19, 18, 21, 22, 6, 10, 60, 1,
            45]


@pytest.fixture(scope="module")
def sap_file_1_table_header_text(test_data_folder):
    return r'| DocumentNo    |CO object name  |CoCode|Cost C|Cost Elem|Cost element descr.                     ' \
           r'|Cost element name     |Created on|Doc. Date |Document Header Text|DocTyp|Group Name     ' \
           r'|Material          |Object Group name                       | Per|Plant    |PUM|Postg Date|' \
           r'Purchase order text     |Purchasing Document       |RCur|     Total quantity|User Name         |' \
           r'      Val.in rep.cur.|Aux. acct assignment_1|D/C   |ParCost.  |' \
           r'CO partner object name                                      |V|' \
           r'Name                                         |'


@pytest.fixture(scope="module")
def get_file_parser_sap_file_1(sap_file_1):
    file_info = FileAnalyzer(file_path=sap_file_1.path, expected_columns_count=sap_file_1.columns_count)
    file_info.header_mapper = HeaderMapping(rows=file_info.file_head_rows,
                                            columns_count=file_info.expected_columns_count)
    return file_info


@pytest.fixture(scope="module")
def get_path_sap_file_2_lager(test_data_folder):
    return os.path.join(test_data_folder, 'sap_file_2_lagefile.txt')


@pytest.fixture(scope="module")
def get_path_sap_file_3_1gb_file(test_data_folder):
    return os.path.join(test_data_folder, 'sap_file_3_1gb_file.txt')


@pytest.fixture(scope="module")
def get_path_sap_file_4_mega_file(test_data_folder):
    return os.path.join(test_data_folder, 'sap_file_4_mega_file.txt')


@pytest.fixture(scope='module')
def get_path_header_exception_file(test_data_folder):
    return os.path.join(test_data_folder, 'test_header_mapping_rows_exception.csv')

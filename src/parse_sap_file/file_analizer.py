from collections import Counter
from itertools import islice
from typing import Type

from charset_normalizer import from_path

from src.parse_sap_file.get_header_mapping import HeaderMapping


class FileAnalyzer:
    def __init__(self, file_path, expected_columns_count=0, delimiter='|', file_head_preprocess_row_count=1000):

        self.delimiter = delimiter
        self.file_head_preprocess_row_count = file_head_preprocess_row_count
        self.expected_columns_count = expected_columns_count
        self.file_path = file_path
        self.file_encoding = self.get_file_encoding(self.file_path)
        self.file_head_rows = self.get_file_head_rows(n_rows=self.file_head_preprocess_row_count)

        self.columns_counts: list = []
        self.aggregate_columns_count: dict = dict()
        self.best_guess_columns_count = 0

        self._header_mapper: HeaderMapping = None
        self._table_header_list: str = str()
        self._table_header_text: str = str()
        self._column_header_mapping: list = list()
        self.header_maps = None

    @property
    def header_mapper(self) -> HeaderMapping:
        return self._header_mapper

    @header_mapper.setter
    def header_mapper(self, header_mapper):
        if type(header_mapper) == HeaderMapping:
            self._header_mapper = header_mapper
            self._table_header_text = self._header_mapper.column_row_text
            self._column_header_mapping = self._header_mapper.header_columns_mapping
            self._table_header_list = self._header_mapper.column_row_text.split(self.delimiter)[1:-1]
            self.header_maps = None

    def guess_columns_count(self):
        for line in reversed(self.file_head_rows):
            row = line.strip()
            if row == '':
                continue
            if row[0] == self.delimiter and row[-1] == self.delimiter:
                self.columns_counts.append(len(row[1:-1].split(self.delimiter)))

        self.aggregate_columns_count = Counter(self.columns_counts)
        self.best_guess_columns_count = max(self.aggregate_columns_count, key=self.aggregate_columns_count.get)
        return self.best_guess_columns_count

    @staticmethod
    def get_file_encoding(file_to_detect):
        enc = from_path(file_to_detect, cp_isolation=['utf-8', 'latin-1'], explain=False).best()
        return enc.encoding

    def get_file_head_rows(self, n_rows, read_mode='r') -> list:
        with open(self.file_path, mode=read_mode, encoding=self.file_encoding) as file_handler:
            return list(islice(file_handler, n_rows))

    @property
    def table_header_text(self):
        return self._table_header_text

    @property
    def column_header_mapping(self):
        return self._column_header_mapping

    @property
    def table_header_list(self):
        return self._table_header_list

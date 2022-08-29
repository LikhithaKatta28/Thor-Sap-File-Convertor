import csv
from dataclasses import dataclass
import logging
import os

from src.parse_sap_file.file_analizer import FileAnalyzer


class ParseFileManager:

    def __init__(self, source_file_path: str, target_file_path: str = None, file_analyzer: FileAnalyzer = None,
                 output_extension: str = 'csv', delimiter: str = '|'
                 , output_header: bool = True):

        self.file_path = source_file_path
        self.target_file_path = target_file_path
        self._file_info = file_analyzer
        self.expected_columns_count: int = self._file_info.expected_columns_count
        self.delimiter = delimiter
        self.target_file_header: bool = output_header
        self.lines_counter: int = 0

        self._output_file_handler = None
        self._input_file_handler = None
        self._output_extension = output_extension
        self._get_target_file_path()

    def __del__(self):
        if self._input_file_handler:
            self._input_file_handler.close()
        if self._output_file_handler:
            self._output_file_handler.close()

    def _get_target_file_path(self):
        if not self.target_file_path:
            file_base_name = os.path.basename(self.file_path)
            filename, ext = os.path.splitext(file_base_name)
            root = os.path.dirname(self.file_path)

            if self._output_extension[0] != '.':
                self._output_extension = '.' + self._output_extension

            self.target_file_path = os.path.join(root, filename + self._output_extension)
            logging.info(f'output saved as: {self.target_file_path}')

        return self.target_file_path

    def _get_files_handlers(self):
        self._output_file_handler = open(self.target_file_path, mode='w', encoding='utf8', newline='')
        self._input_file_handler = open(self.file_path, mode='r', encoding=self._file_info.file_encoding)

    def pars(self) -> int:

        self._get_files_handlers()

        if self.target_file_header:
            parsing_result = self.clean_row(self._file_info.table_header_list)
            self.target_writer(parsing_result, quoting=csv.QUOTE_ALL)

        self.lines_counter: int = self.parser(self.source_file_reader(self._input_file_handler), self.target_writer)

        self._output_file_handler.close()
        self._input_file_handler.close()

        return self.lines_counter

    def target_writer(self, parsing_result, quoting=csv.QUOTE_MINIMAL):
        cw = csv.writer(self._output_file_handler, delimiter=',', quotechar='"', quoting=quoting)
        cw.writerow(parsing_result)

    @staticmethod
    def source_file_reader(file_object):
        while True:
            data = file_object.readline()
            if not data:
                break
            yield data.rstrip('\n')

    def parser(self, source_file_reader, print_result_row):
        lines_counter: int = 0
        parsed_line: list = []
        for file_line in source_file_reader:
            lines_counter += 1
            valid_line = self._get_valid_line(file_line)

            if valid_line:
                parsed_line = self._parse_line(valid_line)

            if parsed_line:
                print_result_row(parsed_line)

        return lines_counter

    def _parse_line(self, valid_line):

        row_in_processing = valid_line.split(self.delimiter)[1:-1]

        if len(row_in_processing) == self.expected_columns_count:
            return self.clean_row(row_in_processing)

        if len(row_in_processing) < self.expected_columns_count:
            return

        if len(row_in_processing) > self.expected_columns_count:
            row_in_processing = handle_extra_delimiter(
                row_in_processing,
                self._file_info.column_header_mapping,
                self.delimiter)
            return self.clean_row(row_in_processing)

    def _get_valid_line(self, file_line):

        if file_line == '':
            return

        if file_line == self._file_info.table_header_text:
            return

        if file_line[0] != self.delimiter and file_line[-1] != self.delimiter:
            return

        return file_line

    @staticmethod
    def clean_row(row_in_processing):
        return [cell.strip() for cell in row_in_processing]

    def normalize_row(self, input_text):
        """Not implemented.
        This works if value columns len is not > header len in mapping
        implement :
        if len(row_in_processing) > expected_column_count:
            row_in_processing = self.normalize_row(raw_line[1:-1])
        """
        output = []
        for column_len in self._file_info.header_maps.header_columns_mapping:
            output.append(input_text[:column_len])
            input_text = input_text[(column_len + 1):]

        return output


def handle_extra_delimiter(row_in_processing, header_columns_mapping, delimiter):
    @dataclass
    class ConcatenationTracker:
        cell_len: int
        concat_cell: str

    normalized_pipes: list = list()
    processed_columns_count = 0

    for columns_index, expected_column_len in enumerate(header_columns_mapping):
        concat = ConcatenationTracker(0, '')
        remaining_columns_to_process = row_in_processing[columns_index + processed_columns_count:]

        for cell_index, cell in enumerate(remaining_columns_to_process):

            concat.cell_len += len(cell)
            concat.concat_cell += cell

            if concat.cell_len >= expected_column_len:
                normalized_pipes.append(concat.concat_cell)
                processed_columns_count += cell_index
                break

            concat.concat_cell += delimiter
            concat.cell_len += 1

    if not normalized_pipes or len(normalized_pipes) != len(header_columns_mapping):
        raise Exception(f'Handling extra pipes failed on line \n{row_in_processing}')

    return normalized_pipes

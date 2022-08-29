import logging
import os
import sys
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')


class PipesAnalyzer:
    def __init__(self):
        self.processed_files_count: int = 0
        self.row_len: dict = dict()
        self.pipes_count: dict = dict()
        self.total_rows: int = int()
        self.valid_rows: int = int()

    def reset_stats_variables(self):
        self.row_len = {}
        self.pipes_count = {}
        self.total_rows = 0
        self.valid_rows = 0

    def get_file_pipes_stats(self, f_path):
        with open(f_path, 'r', encoding='utf-8') as f:
            while True:
                row = f.readline()
                self.total_rows += 1
                if not row:
                    break
                if row[0] != '|':
                    continue
                self.valid_rows += 1
                self.aggregate_row_len(row)
                self.aggregate_col_count(row)

    def aggregate_col_count(self, row):
        key_count = str(len(row[1:-1].split('|')))
        if key_count in self.pipes_count:
            self.pipes_count[key_count] += 1
        else:
            self.pipes_count[key_count] = 1

    def aggregate_row_len(self, row):
        key_len = len(row)
        if key_len in self.row_len:
            self.row_len[key_len] += 1
        else:
            self.row_len[key_len] = 1

    def analyze_folder(self, source):
        arr = os.listdir(source)

        for file in arr:
            start = time.time()

            self.reset_stats_variables()
            if not file.lower().endswith('.txt'):
                continue
            self.processed_files_count += 1
            file_path = os.path.join(source, file)
            self.get_file_pipes_stats(file_path)

            files_stat = self.prepare_statistics_msg()

            logging.info(f'#{self.processed_files_count};'
                         f'{file};'
                         f'timer:{(time.time() - start):.2f};'
                         f'rows_total:{self.total_rows};'
                         f'rows_valid:{self.valid_rows};'
                         f'valid/total:{(self.valid_rows / self.total_rows):.2%};'
                         f'row_length:{files_stat[0]};'
                         f'pipes_count:{files_stat[1]}')

    def prepare_statistics_msg(self):

        rows_len = [(val[0], val[1]) for val in self.row_len.items()]
        rows_len.sort(key=lambda tup: tup[0])
        rows_len = dict(rows_len)

        pipes_counts = [(int(val[0]), val[1]) for val in self.pipes_count.items()]
        pipes_counts.sort(key=lambda tup: tup[0])
        pipes_counts = dict(pipes_counts)

        return rows_len, pipes_counts


if __name__ == '__main__':

    if len(sys.argv) == 1:
        sys.exit("No folder provided. Pipe required valid folder path")
    arg = ''
    try:
        arg = sys.argv[1]
    except Exception as e:
        logging.error(e.args)
    root_fld = os.path.normpath(arg)
    logging.info(f'Running pipes analyzer on folder: {root_fld}')

    pip_analyzer = PipesAnalyzer()
    pip_analyzer.analyze_folder(root_fld)

import logging
import re

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')


class HeaderMapping:
    """
        looking for table header based on first left chars
        patter ins + | +  (plus, pipe, plus)
        Checking to ignore fake summary table present in some files.
        Triggered if expected columns count =3
        |Field name  |From      |To        |
        :param columns_count: expected columns count
        :return: list with columns chars length int
    """

    def __init__(self, rows: list, columns_count: int, delimiter: str = '|'):

        self._delimiter = delimiter
        self.header_columns_mapping: list = []
        self.column_row_text = None
        self.columns_names: list = []
        self._rows = rows
        self._columns_count = columns_count
        self._get_header_mapping()

    def _get_header_mapping(self):

        header_rows_check = [False, False, False]

        for row in self._rows:

            row_strip = row.strip()
            if row_strip[:1] not in ['+', '|']:
                continue

            if row_strip[:1] == '+' and len(row.split('+')) != (self._columns_count + 2):
                continue

            if row_strip[:1] == '|' and len(row.split('|')) != (self._columns_count + 2):
                continue

            # row start +, columns count as in config this can be header
            if not header_rows_check[0]:
                header_rows_check[0] = True
                continue

            if row_strip[:1] == '|':
                row_header_columns_text = row_strip

                row_columns_split = row_header_columns_text.split('|')

                if len(row_columns_split) == (self._columns_count + 2):
                    if len(row_columns_split) == 5:
                        summary_table_re_patter = r'\|Field name\s*\|From\s*\|To\s*\|'
                        logging.info(
                            f'Columns count == {self._columns_count}, check if table is not \n '
                            f'"|Field name  |From      |To        |" \n with regex {summary_table_re_patter}')

                        summary_table_check = re.match(
                            summary_table_re_patter, row)
                        logging.info(
                            f'Check for table is {summary_table_check} ')

                        if summary_table_check:
                            logging.info(
                                f'{summary_table_re_patter} \n match \n {row}. This is not data table! Skipping')
                            header_rows_check[0] = False
                            header_rows_check[1] = False
                            header_rows_check[2] = False
                            self.column_row_text = ''
                            del row_columns_split
                            continue

                    header_rows_check[1] = True
                    self.column_row_text = row_header_columns_text
                    self.columns_names = row_header_columns_text.split(self._delimiter)[1:-1]
                    self.header_columns_mapping = [len(_) for _ in row_columns_split[1:-1]]
                    continue

            if row.strip()[:1] == '+':
                break

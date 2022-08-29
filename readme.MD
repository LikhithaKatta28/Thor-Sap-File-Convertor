# Thor sap file converter

* sap file conversion from raw sap file pipe delimited, pipe prefixed/suffixed, non escaped delimiter
* output of ThorSapFileConverter is proper delimited file, by default comma.
* As you can expect delimiters as values are escaped with double quotes. This way dreaded extra pipe problem is solved.

## Disclaimer

* This is "interna tool for now" / "safeties are off": it is assumed that path is valid and input file is valid sap.
  File should hold expected column count and that target path is valid fld.

## Architecture

The tool is composed out of 3 separate features.

1) `file_analyzer.py` - `file_path, expected_columns_count=0, delimiter='|', file_head_preprocess_row_count=1000`
    1) read (by default) first 1000 rows of file and look for valid +,|,+ patter for first chars in 3 consecutive rows.
       this proofs to be sufficient to identifier header row text and columns names.
    2) with flag `-a`, `--analyze` it will try to guess what it propre header count.
2) `get_header_mapping.py` - needs `self, rows: list, columns_count: int, delimiter: str = '|'`
    1) Instance will return header rows mapping with length of each column
       [15, 16, 6, 6, 9, 40, 22, 10, 10, 20, 6, 15, 18, 40, 4, 9, 3, 10, 24, 26, 4, 19, 18, 21, 22, 6, 10, 60, 1,45]

```
+---------------+----------------+------+------+---------+----------------------------------------+----------------------+----------+----------+--------------------+------+---------------+------------------+----------------------------------------+----+---------+---+----------+------------------------+--------------------------+----+-------------------+------------------+---------------------+----------------------+------+----------+------------------------------------------------------------+-+---------------------------------------------+
| DocumentNo    |CO object name  |CoCode|Cost C|Cost Elem|Cost element descr.                     |Cost element name     |Created on|Doc. Date |Document Header Text|DocTyp|Group Name     |Material          |Object Group name                       | Per|Plant    |PUM|Postg Date|Purchase order text     |Purchasing Document       |RCur|     Total quantity|User Name         |      Val.in rep.cur.|Aux. acct assignment_1|D/C   |ParCost.  |CO partner object name                                      |V|Name                                         |
+---------------+----------------+------+------+---------+----------------------------------------+----------------------+----------+----------+--------------------+------+---------------+------------------+----------------------------------------+----+---------+---+----------+------------------------+--------------------------+----+-------------------+------------------+---------------------+----------------------+------+----------+------------------------------------------------------------+-+---------------------------------------------+
| 572604828     |1045 SU EC ANT #|4457  |70198 |31902001 |Other Material Costs - Engineering Spare|Oth.MatCost Spare     |05.12.2021|01.12.2021|ZPM2-77354          |WA    |ECSUANTPC      |1000168549        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |EA |01.12.2021|                        |                          |USD |             1.000 |MENENDE_K002      |              940.14 |                      |D     |          |                                                            |4|31902001 - CONSUMO SEM48                     |
| 573772508     |1045 SU EC ANT #|4457  |70198 |31902001 |Other Material Costs - Engineering Spare|Oth.MatCost Spare     |13.12.2021|09.12.2021|ZPM1-77389          |WA    |ECSUANTPC      |1000171892        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |EA |09.12.2021|                        |                          |USD |             1.000 |MENENDE_K002      |               18.02 |                      |D     |          |                                                            |4|31902001 - CONSUMO SEM49                     |
| 575576917     |1045 SU EC ANT #|4457  |70198 |31931001 |Other Material Costs - Other            |Other Material Cost   |23.12.2021|16.12.2021|INS-77413           |WA    |ECSUANTPC      |1000172318        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |KG |16.12.2021|                        |                          |USD |            48.000-|MENENDE_K002      |               32.64-|                      |C     |          |                                                            |4|REVERSI#N ERROR EN CODIGOS SAP PAPELETA      |
| 572616072     |1045 SU EC ANT #|4457  |70198 |31931001 |Other Material Costs - Other            |Other Material Cost   |05.12.2021|02.12.2021|INS-76999           |WA    |ECSUANTPC      |1000168780        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |KG |02.12.2021|                        |                          |USD |            48.000 |MENENDE_K002      |              185.63 |                      |D     |          |                                                            |4|31931001 - CONSUMO SEM48                     |
| 573754086     |1045 SU EC ANT #|4457  |70198 |31931001 |Other Material Costs - Other            |Other Material Cost   |13.12.2021|13.12.2021|INS-77000           |WA    |ECSUANTPC      |1000168780        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |KG |13.12.2021|                        |                          |USD |            48.000 |MENENDE_K002      |              185.70 |                      |D     |          |                                                            |4|31931001 - CONSUMO SEM49                     |
| 573754086     |1045 SU EC ANT #|4457  |70198 |31931001 |Other Material Costs - Other            |Other Material Cost   |13.12.2021|13.12.2021|INS-77000           |WA    |ECSUANTPC      |1000168780        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |KG |13.12.2021|                        |                          |USD |            48.000 |MENENDE_K002      |              185.70 |                      |D     |          |                                                            |4|31931001 - CONSUMO SEM49                     |
| 573757059     |1045 SU EC ANT #|4457  |70198 |31931001 |Other Material Costs - Other            |Other Material Cost   |13.12.2021|07.12.2021|INS-77403           |WA    |ECSUANTPC      |1000168780        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |KG |07.12.2021|                        |                          |USD |            72.000 |MENENDE_K002      |              278.55 |                      |D     |          |                                                            |4|31931001 - CONSUMO SEM49                     |
| 573758951     |1045 SU EC ANT #|4457  |70198 |31931001 |Other Material Costs - Other            |Other Material Cost   |13.12.2021|08.12.2021|INS-77404           |WA    |ECSUANTPC      |1000168780        |EC SU Antartida PCost CRG25 (SR)        |  12|1045     |KG |08.12.2021|                        |                          |USD |            48.000 |MENENDE_K002      |              185.70 |                      |D     |          |                                                            |4|31931001 - CONSUMO SEM49                     |
```

3) `parse_manager.py` - `source_file_path: str, target_file_path: str = None, file_analyzer: FileAnalyzer = None,
   output_extension: str = 'csv', delimiter: str = '|', output_header: bool = True`
    1) Parser manager - will....manage parsing process.
       Flow is based on mail loop and two files engines. First engine will read source (-s) file one row at the time.
       Process it and save to putput file, one row at the time.
       In case extra pipe will be encountered in any data row, parse will manage it using header mapping.

## Environment

* Python 3.10+

TIP❗ `concider setting windows environment variables for to simplyfie cmd operation
s1p ==> \\S2.ms.unilever.com\dfs\ES-GROUPS\cor\frd\UFO-General\INTERFACE\S1P
P1P ==> \\S2.ms.unilever.com\dfs\ES-GROUPS\cor\frd\UFO-General\INTERFACE\P1P
in cmd you can refear ty variables with percentails _%S1P%_ and respectivly _%P1P%_ `

Create env with python 3,10+

In Unilever environment it is recommended to store packages in shared location for your security, and it is needed for
deployment to production/QA servers. 3rd party developers don't need adjust to this rule.

```command
py -3.10 -m venv env
env\scripting\activate
py -m pip install -r requirements.txt --no-index --find-links=\\s2.ms.unilever.com\dfs\ES-GROUPS\cor\frd\UFO-General\INTERFACE\UPIT\pypi\
py -m pip install --upgrade pip
```

It is recommended to store all wheels on internal scfs folder.
\\s2.ms.unilever.com\dfs\ES-GROUPS\cor\frd\UFO-General\INTERFACE\UPIT\pypi\
Remember to save whl on pypi folder to user --find-links=

```command
py -m pip download -r requirements.txt -d \\s2.ms.unilever.com\dfs\ES-GROUPS\cor\frd\UFO-General\INTERFACE\UPIT\pypi\
```

## Testing

Due to significant size of some test files, you will not find them in this repo.
For test files goto : \\S2.ms.unilever.com\dfs\ES-GROUPS\cor\frd\UFO-General\INTERFACE\THOR\thor_sap_file_converter
or create our own files with names

    sap_file_2_lagefile.txt
    sap_file_3_1gb_file.txt
    sap_file_4_mega_file.txt

just remember to update `conftest.py` expected columns count.

Execute
`py -m pytest -vv`

## Usage

There are two scenarios that can be handlet:

1) analyze provided file and look for most likely column count in header
   `py parse_sap_file.py -s "%s1p%\sap_fole_name.txt" -a`
   output will be integer value of column count, this will be useful on main feature of this application

2) Pars SAP file into csv file

* most control is achieved by providing 3 params

- **s** full path to source file
- **c** expected column count
- **t** target file path with csv extension

`py parse_sap_file.py -s %s1p%\sap_fole_name.txt -c (expected_column_count : int) -t "target/for_converted.csv"`
`py parse_sap_file.py -s "%s1p%\UFO_NPD_BOT_ROUTING.20220510062351.340619.00CBM6tN.S1P.txt" -c 42 -t "C:\1\UFO_NPD_BOT_ROUTING.20220510062351.340619.00CBM6tN.S1P.csv"`


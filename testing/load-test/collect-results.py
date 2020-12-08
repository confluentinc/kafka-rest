#!/bin/python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This script aggregates all the input k6 JSON test summaries into a single spreadsheet.
# Usage: python collect-results.py test1.out test2.out test3.out summary.xlsx


from builtins import (dict, float, int, len, list, max, open, range, sorted, str)

from collections import defaultdict

import argparse
import json
import xlsxwriter


parser = argparse.ArgumentParser()
parser.add_argument('filenames', metavar='N', type=str, nargs='+')

args = parser.parse_args()

table = defaultdict(lambda: defaultdict(lambda: defaultdict(str)))
columns = dict()

for filename in args.filenames[:-1]:
    result = json.load(open(filename))
    test_name = filename[filename.rfind('/') + 1:-4]

    for (metric_name, metrics) in result['metrics'].items():
        table[test_name][metric_name] = metrics
        columns[metric_name] = list(metrics.keys())

workbook = xlsxwriter.Workbook(args.filenames[-1])
worksheet = workbook.add_worksheet()

test_name_format = workbook.add_format({'align': 'left'})
header_format = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
value_format = workbook.add_format({'num_format': 2})

worksheet.set_column(0, 0, len(max(table.keys(), key=len)))

worksheet.merge_range(
        first_row=0,
        first_col=0,
        last_row=1,
        last_col=0,
        data='Test Name',
        cell_format=header_format)

row, col = 0, 1
for column_name in sorted(columns.keys()):
    worksheet.merge_range(
            first_row=row,
            first_col=col,
            last_row=row,
            last_col=col + len(columns[column_name]) - 1,
            data=column_name,
            cell_format=header_format)
    worksheet.set_column(
            col,
            col + len(columns[column_name]) - 1,
            max(8, int(len(column_name) / len(columns[column_name]))))

    for i in range(col, col + len(columns[column_name])):
        worksheet.write_string(row + 1, i, columns[column_name][i - col], header_format)

    col += len(columns[column_name])

common_latency_metrics = [
    'http_req_blocked',
    'http_req_connecting',
    'http_req_duration',
    'http_req_receiving',
    'http_req_sending',
    'http_req_tls_handshaking',
    'http_req_waiting',
    'iteration_duration',
]
common_count_metrics = [
    'checks',
    'http_reqs',
    'iterations',
    'vus',
    'vus_max'
]

row = 2
for test_name in table.keys():
    worksheet.write_string(row, 0, test_name, test_name_format)

    col = 1
    for column_name in sorted(columns.keys()):
        for i in range(col, col + len(columns[column_name])):
            value = table[test_name][column_name][columns[column_name][i - col]]
            if not value:
                parsed_value = None
            elif column_name.endswith('Latency') or column_name in common_latency_metrics:
                parsed_value = float(value)
            elif column_name.endswith('Count') or column_name in common_count_metrics:
                parsed_value = float(value)
            else:
                parsed_value = value
            worksheet.write(row, i, parsed_value, value_format)

        col += len(columns[column_name])

    row += 1

workbook.close()

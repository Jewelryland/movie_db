#!/usr/bin/env python2.7
"""Extract the release year from the movie title in the csv file and
add it as another column.

Author: Rohith Subramanyam <rohithvsm@cs.wisc.edu>

"""

import sys
import os
import re
import csv

def add_year(in_csv_file, out_csv_file):
    """Parse the csv file and extract read the year from the movie title
    and add it as another comma-separated field.

    Args:
        in_csv_file : path to the input csv file
        out_csv_file: path to the output csv file

    """

    mov_title_yr_regex = re.compile('.*(\(.*\))$')
    regex_mismatches = 0
    yr_field_pos = 2

    with open(in_csv_file, 'rb') as in_csv, open(out_csv_file,'wb') as out_csv:
        csv_reader = csv.reader(in_csv)
        csv_writer = csv.writer(out_csv, lineterminator='\n')
        header_row = csv_reader.next()
        header_row.insert(yr_field_pos, 'year')
        csv_writer.writerow(header_row)
        for row in csv_reader:
            title = row[1]
            match_result = mov_title_yr_regex.search(title)
            if match_result is not None:
                year = match_result.groups()[0].lstrip('(').rstrip(')')
                row.insert(yr_field_pos, year)
            else:
                regex_mismatches += 1
                row.insert(yr_field_pos, '')
            csv_writer.writerow(row)

    print 'No. of mismatches: %d' % regex_mismatches

def _parse_cmd_line_arg():
    """Parses command-line arguments.

    Returns:
        tuple of path to the input and output csv files

    """

    if len(sys.argv) < 3:
        print >> sys.stderr, ('Usage: %s in_csv_file out_csv_file'
                               % os.path.basename(__file__))
        sys.exit(1)

    return (sys.argv[1], sys.argv[2])

def main():
    """Parse the command-line arguments to get the files and do the
    merge."""

    in_csv_file, out_csv_file = _parse_cmd_line_arg()

    add_year(in_csv_file, out_csv_file)

if __name__ == '__main__':
    main()

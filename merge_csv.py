#!/usr/bin/env python2.7
"""Merges multiple csv files into a single file.

Author: Rohith Subramanyam <rohithvsm@cs.wisc.edu>

"""

import sys
import os

def merge_csv(csv_files, merged_file):
    """Merge the individual csv files in csv_files to a single merged
    file.

    Args:
        csv_files:   list containing the csv files
        merged_file: file into which the merged csv is written

    """

    with open(merged_file, 'wb') as out_fo:
        for index, csv_file in enumerate(csv_files):
            with open(csv_file, 'rb') as csv_fo:
                if index != 0:  # write header only once
                    next(csv_fo)
                for line in csv_fo:
                    print >> out_fo, line.strip()

def _parse_cmd_line_args():
    """Parses command-line arguments.

    Returns:
        a tuple containing the csv files to be merged as the first
        element and the file into which it is merged as the second
        element of the tuple.

    """

    if len(sys.argv) < 4:
        print >> sys.stderr, ('Usage: %s csv_file1 csv_file2 ... csv_filen '
                              'merged_file' % os.path.basename(__file__))
        sys.exit(1)

    csv_files = []

    # last argument is the file into which the contents have to be
    # merged
    for file_ in sys.argv[1:-1]:
        csv_files.append(file_)

    merged_file = sys.argv[-1]

    return (csv_files, merged_file)

def main():
    """Parse the command-line arguments to get the files and do the
    merge."""

    csv_files, merged_file = _parse_cmd_line_args()

    merge_csv(csv_files, merged_file)

if __name__ == '__main__':
    main()

'''
Get help with -h

Example usage:
    python generate_csv.py 10 --delimiter ',' int int float str
'''

import random
import sys
import argparse
import csv
import string


def integer_csv(rows, schema, delimiter):
    random.seed(42)
    generators = []
    char_set = (string.ascii_letters + string.digits +
                '"' + "'" + "#&* \t")

    for column in schema:
        if column == 'int':
            generators.append(lambda: random.randint(0, 1e9))
        elif column == 'str':
            generators.append(lambda: ''.join(
                random.choice(char_set) for _ in range(12)))
        elif column == 'float':
            generators.append(lambda: random.random())

    writer = csv.writer(sys.stdout, delimiter=delimiter)
    for x in xrange(rows):
        writer.writerow([g() for g in generators])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate a large CSV file.',
        epilog='''"Space is big. You just won't believe how vastly,
        hugely, mind-bogglingly big it is."''')
    parser.add_argument('rows', type=int,
                        help='number of rows to generate')
    parser.add_argument('--delimiter', type=str, default=',', required=False,
                        help='the CSV delimiter')
    parser.add_argument('schema', type=str, nargs='+',
                        choices=['int', 'str', 'float'],
                        help='list of column types to generate')

    args = parser.parse_args()
    integer_csv(args.rows, args.schema, args.delimiter)

import random
import sys
import argparse
import csv


def integer_csv(rows, columns, delimiter):
    random.seed(42)
    writer = csv.writer(sys.stdout, delimiter=delimiter)
    for x in xrange(rows):
        writer.writerow(
            [str(random.randint(0, 1e9)) for _ in range(columns)])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate a large CSV file.',
        epilog='''"Space is big. You just won't believe how vastly,
        hugely, mind-bogglingly big it is."''')
    parser.add_argument('rows', type=int,
                        help='number of rows to generate')
    parser.add_argument('columns', type=int,
                        help='number of columns to generate')
    parser.add_argument('delimiter', type=str, default=',', nargs='?',
                        help='the CSV delimiter')

    args = parser.parse_args()
    integer_csv(args.rows, args.columns, args.delimiter)

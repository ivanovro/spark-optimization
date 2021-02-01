import argparse
import os
from itertools import chain
from itertools import islice

import requests


def yield_lines(source):
    if source == 'web':
        response = requests.get("https://stream.meetup.com/2/rsvps",
                                stream=True)
        for line in response.iter_lines():
            yield line.decode('utf-8')
    else:
        with open(source, 'r') as f:
            for line in f:
                yield line


def print_all(source):
    while True:
        for line in yield_lines(source):
            print(line)


def write_all(source, sink):
    lines = yield_lines(source)
    if not os.path.exists(sink):
        os.mkdir(sink)
    for ii, chunk in enumerate(chunks(lines, 100)):
        with open(os.path.join(sink, '{}'.format(ii)), 'w') as f:
            f.writelines(u'\n'.join(chunk))


def chunks(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stream meetup RSVPs.')
    parser.add_argument('--source', default='web', type=str,
                        help="Source: 'web' or path to file")
    parser.add_argument('--sink', default='stdout', type=str,
                        help="Sink: 'stdout' or path to directory")
    args = parser.parse_args()
    if args.sink == 'stdout':
        print_all(args.source)
    else:
        write_all(args.source, args.sink)

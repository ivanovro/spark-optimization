import argparse
import itertools
import random
import time

import matplotlib.pyplot as plt


def print_lines(blobs):
    for x, y, k in generate_blobs(blobs):
        print(x, y, k)
        time.sleep(0.2)


def generate_blobs(blobs):
    std = 0.1
    offset = 0.0

    while True:
        mu = random.randint(0, blobs-1)
        yield (random.gauss(mu, std)+offset, random.gauss(mu, std)+offset, mu)
        offset += 0.005


def plot_moving_clusters(blobs=2, n=100):

    blob_generator = generate_blobs(blobs)
    x, y, _ = zip(*itertools.islice(blob_generator, n))

    fig, ax = plt.subplots()
    scatter = ax.scatter(x, y, c=list(range(100)), cmap='plasma')
    fig.colorbar(scatter, label='time')
    ax.set_xlabel('x')
    ax.set_ylabel('y')
    ax.set_title('Moving clusters')

    return fig


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate 2D blobs points.')
    parser.add_argument('blobs', type=int, help='Number of blobs')
    args = parser.parse_args()
    print_lines(args.blobs)

import sys
from datetime import datetime
import time
from typing import Tuple

import numpy as np

Point = Tuple[float, float]


def random_walk(start: Point, step: float) -> Point:
    while True:
        yield start
        angle = 2 * np.pi * np.random.uniform()
        x, y = np.cos(angle), np.sin(angle)
        start = (start[0] + x * step, start[1] + y * step)


def offset_gaussian(mean: Point = (0., 0.),
                    stddev: Point = (1., 1.),
                    offset: Point = (0., 0.)) -> Point:
    mean_x, mean_y = mean
    stddev_x, stddev_y = stddev
    offset_x, offset_y = offset
    return (np.random.normal(mean_x + offset_x, stddev_x),
            np.random.normal(mean_y + offset_y, stddev_y))


def points_with_outliers(mean: Point = (0., 0.),
                         stddev: Point = (1., 1.),
                         start: Point = (0., 0.),
                         step: float = 0.1,
                         outlier: Point = (20, 20)) -> Point:
    for offset in random_walk(start, step):
        if np.random.uniform() < 0.95:
            yield offset_gaussian(mean, stddev, offset)
        else:
            yield offset_gaussian(outlier, stddev, offset)


def post_points(timestamp=False):
    for x, y in points_with_outliers(step=0.1, outlier=(12, 12)):
        if timestamp:
            now = datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
            string = "{0} {1} {2}".format(x, y, now)
        else:
            string = "{0} {1}".format(x, y)
        print(string)
        time.sleep(0.02)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        post_points(timestamp=True)
    else:
        post_points()

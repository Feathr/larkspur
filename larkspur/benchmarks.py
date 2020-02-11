#!/usr/bin/env python3

import sys
import time
import math
import redis
from bson import ObjectId
from larkspur import BloomFilter


def main(capacity=10000, error_rate=0.01):
    r = redis.StrictRedis(db=4)
    r.flushdb()
    bf = BloomFilter(r, 'benchmark', capacity=capacity, error_rate=error_rate)
    members = [str(ObjectId()) for x in range(capacity)]
    start = time.time()
    bf.bulk_add(members)
    end = time.time()
    duration = end - start
    rate = bf.capacity / duration
    print(f'{duration:5.3f} seconds to add to capacity, {rate:10.2f} entries/second')
    trials = bf.capacity
    false_positives = 0
    nonmembers = [str(ObjectId()) for x in range(trials)]
    start = time.time()
    for nonmember in nonmembers:
        if nonmember in bf:
            false_positives += 1
    end = time.time()
    duration = end - start
    rate = trials / duration
    observed_error_rate = false_positives / float(trials)
    theoretical_error_rate = math.pow(
        (1 - math.exp(-bf.num_slices * (bf.capacity + 0.5) / (bf.num_bits - 1))),
        bf.num_slices
    )
    print(f'{duration:5.3f} seconds to check false positives, {rate:10.2f} checks/second')
    print(f'Requested error rate: {bf.error_rate:2.4f}')
    print(f'Observed error rate: {observed_error_rate:2.4f}')
    print(f'Projected error rate (Goel/Gupta): {theoretical_error_rate:2.4f}')


if __name__ == '__main__':
    args = sys.argv[1:]
    capacity = 10000
    error_rate = 0.01
    if len(args):
        capacity = int(sys.argv[1])
        error_rate = float(sys.argv[2])
    main(capacity, error_rate)

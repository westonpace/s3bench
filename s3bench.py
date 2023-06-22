import argparse
import math
import time

from typing import NamedTuple

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import pyarrow as pa
import pyarrow.fs as fs

class Config(NamedTuple):
    region: str
    bucket: str
    prefix: str
    endpoint: str

def get_or_die(obj, attr):
    if attr in obj:
        return obj[attr]
    self._fail(f"Expected required attribute {attr} in config file")

def get_or_else(obj, attr, default):
    if attr in obj:
        return obj[attr]
    return default

def load_config(config_file):
    with open(config_file, "r") as f:
        obj = load(f, Loader)
        region = get_or_else(obj, "region", "us-east-1")
        bucket = get_or_die(obj, "bucket")
        prefix=  get_or_else(obj, "prefix", "")
        endpoint = get_or_else(obj, "endpoint", None)
        return Config(region, bucket, prefix, endpoint)

def create_fs(config):
    return fs.S3FileSystem(region=config.region, endpoint_override=config.endpoint)

def list_all(filesystem, config):
    base_path = config.bucket
    if config.prefix:
        base_path = base_path + "/" + config.prefix
    selector = fs.FileSelector(base_path, recursive=True)
    filesystem.get_file_info(selector)

operations = {
    'list_all': list_all
}

class Stats(NamedTuple):
    min: float
    mean: float
    max: float
    stddev: float

def calculate_stats(times):
    minimum = min(times)
    maximum = max(times)
    mean = sum(times) / len(times)
    stddev = math.sqrt(sum([(t - mean) ** 2 for t in times]) / len(times))
    return Stats(minimum, mean, maximum, stddev)

def print_results(operation, config, times):
    stats = calculate_stats(times)
    print(f"Operation: {operation}")
    print(f"Bucket: {config.bucket}")
    print(f"Prefix: {config.prefix}")
    print(f"Min: {stats.min}s")
    print(f"Mean: {stats.mean}s")
    print(f"Max: {stats.max}s")
    print(f"Standard Deviation: {stats.stddev}s")

def run_benchmark_once(config, fs, op, itr_idx):
    print(f"Iteration {itr_idx}: ", end=None)
    start = time.time()
    op(fs, config)
    end = time.time()
    print(f"{end - start}s")
    return end - start

def run_benchmark(config, operation, num_iters):
    fs = create_fs(config)
    op = operations[operation]
    print(f"Starting benchmark: {operation}")
    times = []
    for itr_idx in range(num_iters):
        times.append(run_benchmark_once(config, fs, op, itr_idx))

    print_results(operation, config, times)

def main():
    parser = argparse.ArgumentParser(
                    prog='S3Benchmark',
                    description='Benchmarks various S3FS operations in pyarrow')
    parser.add_argument('config_file', help="a yaml file describing the S3 repository")
    parser.add_argument('operation', help="the operation to benchmark", choices=sorted(operations.keys()))
    parser.add_argument('--num_iters', default=10, type=int, help='the number of times to run the benchmark')

    args = parser.parse_args()
    config = load_config(args.config_file)
    run_benchmark(config, args.operation, args.num_iters)

if __name__ == '__main__':
    main()
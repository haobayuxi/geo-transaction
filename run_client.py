#! /usr/bin/env python
import os
import subprocess
import sys
import logging
from time import sleep
import pexpect
from logging import Logger
from aggregate_result import read_latency_results, read_throughput_results
from ruamel import yaml


TMP_DIR = '~/'

IP = "192.168.50.12"

logging.basicConfig()
logger = logging.getLogger('')

passwd_key = "hpcgrid3102\n"


def read_local_throughput_results(ClientNum):
    result = 0.0
    result_file_name = "throughput"
    result_file = open(result_file_name, 'w')
    result_latency_file_name = "latency"
    result_latency_file = open(result_latency_file_name, 'w')
    i = 0
    while i < ClientNum:
        file_name = str(i)+"throughput.data"
        latency_name = str(i)+"latency.data"
        f = open(file_name)
        line = f.readline()
        result += float(line.strip('\n'))
        f.close()
        latency_file = open(latency_name)
        latency = latency_file.readline()
        result_latency_file.write(str(latency))
        latency_file.close()
        os.remove(file_name)
        os.remove(latency_name)
        i += 1
    result_file.write(str(result)+"\n")
    result_file.close()
    result_latency_file.close()


def increase_zipf():
    content = []
    with open('config.yml', "r") as f:
        content = yaml.load(f, Loader=yaml.RoundTripLoader)
        x = content['zipf']
        if x >= 0.95:
            content['zipf'] = 0.5
        else:
            content['zipf'] = x + 0.1
    with open('config.yml', "w") as f:
        yaml.dump(content, f, Dumper=yaml.RoundTripDumper)


def main():
    logger.setLevel(logging.DEBUG)
    logger.info("start")
    read_local_throughput_results()
    file_name = "throughput"
    latency_file_name = "latency"
    os.remove(file_name)
    os.remove(latency_file_name)
    # vim config
    increase_zipf()
    logger.info("done")
    # os.path.isfile(fname)


if __name__ == "__main__":
    main()

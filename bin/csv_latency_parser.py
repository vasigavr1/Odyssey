#!/usr/bin/python

import sys, os, ntpath, getopt
from enum import Enum

"""
========
Parser 4 aggregated over time results
========
"""


class ReqType(Enum):
    RELEASE_REQ = 0
    ACQUIRE_REQ = 1
    WRITE_REQ = 2
    READ_REQ = 3
    RMW_REQ = 4


REQ_TYPE_NUM = 5


class LatencyParser:
    def __init__(self):
        self.latency_values = []

        self.requests = [[] for i in range(REQ_TYPE_NUM)]
        self.max_req_latency = [0] * REQ_TYPE_NUM
        self.per_req_count = [0] * REQ_TYPE_NUM
        self.all_reqs = []
        self.parseInputStats()
        self.printAllStats()

    def printStats(self, array, max_latency):
        self.avgLatency(array)
        # self.percentileLatency(array, 20)
        self.percentileLatency(array, 50)
        self.percentileLatency(array, 90)
        # self.percentileLatency(array, 95)
        self.percentileLatency(array, 99)
        # self.percentileLatency(array, 99.9)
        # self.percentileLatency(array, 99.99)
        # self.percentileLatency(array, 99.999)
        # self.percentileLatency(array, 99.9999)
        # self.percentileLatency(array, 100)
        print("Max Latency: ", max_latency, "us")

    def printAllStats(self):
        for type_i in range(REQ_TYPE_NUM):
            if self.per_req_count[type_i] > 0:
                print("~~~~~~ " + ReqType(type_i).name + " Stats ~~~~~~~")
                self.printStats(self.requests[type_i], self.max_req_latency[type_i])

        print("\n~~~~~~ Overall Stats ~~~~~~~~~")
        self.printStats(self.all_reqs, max(self.max_req_latency))

    def avgLatency(self, array):
        cummulative = 0
        total_reqs = 0
        if len(array) > 0:
            for x in range(len(self.latency_values)):
                assert (x < len(self.latency_values))
                assert (x < len(array))
                cummulative = self.latency_values[x] * array[x] + cummulative
                total_reqs += array[x]
        if total_reqs > 0:
            print("Reqs measured: ", total_reqs, "| Avg Latency: ", cummulative / total_reqs)
        else:
            print("No reqs measured")

    def percentileLatency(self, array, percentage):
        total_reqs = 0
        sum_reqs = 0
        if len(array) > 0:
            for x in range(len(self.latency_values)):
                # cummulative = self.latency_values[x] * array[x] + cummulative
                total_reqs += array[x]
        if total_reqs > 0:
            if percentage == 100:
                for x in reversed(range(len(self.latency_values))):
                    if array[x] > 0:
                        if self.latency_values[x] == -1:
                            print(percentage, "%: >", self.latency_values[x - 1], "us")
                        else:
                            print(percentage, "%: ", self.latency_values[x], "us")
                    return
            else:
                for x in range(len(self.latency_values)):
                    sum_reqs += array[x]
                    if ((100.0 * sum_reqs) / total_reqs) >= percentage:
                        if self.latency_values[x] == -1:
                            print(percentage, "%: >", self.latency_values[x - 1], "us")
                        else:
                            print(percentage, "% : ", self.latency_values[x], "us")
                        return
        else:
            print("No reqs measured")

    def parseInputStats(self):
        lr_lines = 0
        for type_i in range(REQ_TYPE_NUM):
            self.max_req_latency[type_i] = 0
            self.per_req_count[type_i] = 0

        for line in sys.stdin:  # input from standard input
            if line[0] == '#':
                continue
            (command, words) = line.strip().split(":", 1)
            command = command.strip()
            assert (ReqType[command].value < REQ_TYPE_NUM)
            type_i = ReqType[command].value
            words = words.strip().split(",")
            lat_occurances = int(words[1].strip())
            if words[0].strip() == 'max':
                self.max_req_latency[type_i] = lat_occurances
            else:
                lat_bucket = int(words[0].strip())
                if lat_bucket == 0:
                    print(self.requests)
                    lr_lines = 0
                if lr_lines >= len(self.latency_values):
                    self.latency_values.append(lat_bucket)
                if lr_lines >= len(self.all_reqs):
                    self.all_reqs.append(lat_occurances)
                else:
                    self.all_reqs[lr_lines] = self.all_reqs[lr_lines] + lat_occurances
                print(type_i)
                if lr_lines >= len(self.requests[type_i]):
                    self.requests[type_i].append(lat_occurances)
                else:
                    slef.requests[type_i][lr_lines] = lat_occurances
                self.per_req_count[type_i] += 1
            lr_lines += 1
        # print(self.latency_values)
        # print(self.max_req_latency)
        # print(self.per_req_count)
        print(self.requests)
        # print(max(self.max_req_latency))


if __name__ == '__main__':
    LatencyParser()

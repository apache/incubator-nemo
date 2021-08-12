#!/usr/bin/env python3

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

'''
This script takes the result of the network_profiling script, and parses the results into hierarchical trees.
This takes the result of the qperf library, containing information between each pair of nodes about the network
bandwidth and latency, and takes them to measure the distance ( = latency / bandwidth) and writes the results to
labeldict.json. The labeldict contains information as a [nodename/groupname, distance] pair, where nodename always
has distance of 0, and the following pairs describe the tree dependency between the groups, each labeled with its index.
'''

import json
from scipy.cluster.hierarchy import dendrogram, linkage

with open('result.json') as json_file:
  dic = json.load(json_file)

# Sort according to node ids
# each item looks like: {"willy-3/willy-4": {"bw": "922517504 bytes/sec",  "latency": "36828 ns"}}
sorted_dic = {k: v for k, v in
              sorted([item for item in dic.items() if 'latency' in item[1] and 'bw' in item[1]],
                     key=lambda a: (item[0].split('/')[0], item[0].split('/')[1]))}
slaves = sorted(dic['slaves'].split('/'))
# Upper triangle of the connection graph
dist = [int(v['latency'].split()[0])/int(v['bw'].split()[0]) for v in sorted_dic.values()]
# Use library to generate the linkage matrix
Z = linkage(dist)

# Visualize result
# from matplotlib import pyplot as plt
# fig = plt.figure(figsize=(25, 10))
# dn = dendrogram(Z, labels=slaves)
# plt.show()

labeldict = {idx: (v, "0") for idx, v in enumerate(slaves)}
for item in Z:
  labeldict[len(labeldict)] = ("{}+{}".format(int(item[0]),  int(item[1])), "{:.8f}".format(item[2]))

with open('labeldict.json', 'w') as fp:
  json.dump(labeldict, fp)


# def closest_clusters_between(source, destination, labeldict):
#   for v in labeldict.values():
#     lst = v.split('+')
#     if source in lst and destination in lst:
#       return lst
#
# print("Closest clusters between {} and {} are {}".format('willy-10', 'willy-13', closest_clusters_between('willy-10', 'willy-13', labeldict)))

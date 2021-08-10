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

#
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
from scipy.cluster.hierarchy import dendrogram, linkage

with open('result.json') as json_file:
  dic = json.load(json_file)

# Sort according to node ids
sorted_dic = {k: v for k, v in sorted([a for a in dic.items() if 'latency' in a[1] and 'bw' in a[1]], key=lambda a: (a[0].split('/')[0], a[0].split('/')[1]))}
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

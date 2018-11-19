#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import os
import json
import numpy as np

def main():
   try:
       filepath = sys.argv[1]
   except IndexError:
       print("Please provide the file path for the metric log file.")
   else:
       if not os.path.isfile(filepath):
           print("File path {} does not exist. Exiting...".format(filepath))
           sys.exit()

       metricDictionary = dict()
       vertexToMetricDict = dict()
       with open(filepath, 'r') as fp:
           for line in fp:
               metricInJson = json.loads(line)
               metricKey = metricInJson["computationUnitId"]
               metricDictionary[metricKey] = metricInJson["metricList"]
               if metricKey.find('Task-vertex-') != -1: # Vertex metric
                   vertexIdSuffix = metricKey.split('Task-vertex-')[1]
                   if vertexIdSuffix.find('_') != -1: # physical level metric
                       vertexId = 'vertex-' + vertexIdSuffix.split('_')[0]
                       metricDictList = metricDictionary[metricKey]
                       if isinstance(metricDictList, dict):
                           metricDictList = [metricDictList]
                       for metricDict in metricDictList:
                           for key, value in metricDict.items():
                               if (key != 'EndTime') & (key != 'StartTime'):
                                   vertexMetricDict = vertexToMetricDict.get(vertexId, dict())
                                   vertexMetricDictValueList = vertexMetricDict.get(key, [])
                                   vertexMetricDictValueList.append(value)
                                   vertexMetricDict[key] = vertexMetricDictValueList
                                   vertexToMetricDict[vertexId] = vertexMetricDict

       query_metric = True
       while(query_metric):
           user_input = input("1 - View metric for a computation unit, 2 - View metric for all IR vertices, 3 - exit: ")
           if user_input == "1":
               computationUnitId = input("Enter computation unit ID: ")
               for metric in metricDictionary[computationUnitId]:
                   print(metric)
           elif user_input == "2":
               for vertexId, metricDict in sorted(vertexToMetricDict.items()):
                   print(vertexId)
                   metricKeys, valuesMin, valuesMedian, valuesMax, valuesMean, valuesSum = ['Metric'], ['Min'], ['Median'], ['Max'], ['Mean'], ['Total']
                   for metricKey, metricValues in metricDict.items():
                       metricKeys.append(metricKey)
                       valuesMin.append(str(np.min(metricValues)))
                       valuesMedian.append(str(np.median(metricValues)))
                       valuesMax.append(str(np.max(metricValues)))
                       valuesMean.append(str(np.mean(metricValues)))
                       valuesSum.append(str(np.sum(metricValues)))
                   padding = 1
                   widthKey, widthMin, widthMedian, widthMax, widthMean, widthSum = map(lambda x:len(max(x, key=len)) + padding, [metricKeys, valuesMin, valuesMedian, valuesMax, valuesMean, valuesSum])
                   templete = '{:<%s} {:<%s} {:<%s} {:<%s} {:<%s} {:<%s}' % (widthKey, widthMin, widthMedian, widthMax, widthMean, widthSum)
                   for metricKey, valueMin, valueMedian, valueMax, valueMean, valueSum in zip(metricKeys, valuesMin, valuesMedian, valuesMax, valuesMean, valuesSum):
                    print(templete.format(metricKey, valueMin, valueMedian, valueMax, valueMean, valueSum))
           else:
               print ("Exiting metric parser")
               query_metric = False


if __name__ == '__main__':
   main()

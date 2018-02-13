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
                       for metricDict in metricDictionary[metricKey]:
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
               print ('The "WrittenBytes" metric can be 0 if the data is not serialized and just handled on memory')
               for vertexId, metricDict in sorted(vertexToMetricDict.items()):
                   print(vertexId)
                   metricKeys, valuesMin, valuesMean, valuesMax, valuesSum = ['Metric'], ['Min'], ['Mean'], ['Max'], ['Total']
                   for metricKey, metricValues in metricDict.items():
                       metricKeys.append(metricKey)
                       valuesMin.append(str(np.min(metricValues)))
                       valuesMean.append(str(np.mean(metricValues)))
                       valuesMax.append(str(np.max(metricValues)))
                       valuesSum.append(str(np.sum(metricValues)))
                   padding = 1
                   widthKey, widthMin, widthMean, widthMax, widthSum = map(lambda x:len(max(x, key=len)) + padding, [metricKeys, valuesMin, valuesMean, valuesMax, valuesSum])
                   templete = '{:<%s} {:<%s} {:<%s} {:<%s} {:<%s}' % (widthKey, widthMin, widthMean, widthMax, widthSum)
                   for metricKey, valueMin, valueMean, valueMax, valueSum in zip(metricKeys, valuesMin, valuesMean, valuesMax, valuesSum):
                    print(templete.format(metricKey, valueMin, valueMean, valueMax, valueSum))
           else:
               print ("Exiting metric parser")
               query_metric = False


if __name__ == '__main__':
   main()

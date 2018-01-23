import sys
import os
import json

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
       with open(filepath, 'r') as fp:
           for line in fp:
               metricInJson = json.loads(line)
               metricKey = metricInJson["computationUnitId"]
               metricDictionary[metricKey] = metricInJson["metricList"]

       query_metric = True
       while(query_metric):
           user_input = input("1 - View metric for a computation unit, 2 - exit: ")
           if user_input == "1":
               computationUnitId = input("Enter computation unit ID: ")
               for metric in metricDictionary[computationUnitId]:
                   print("{}: {} -> {} took {} ms".format(metric["ContainerId"], metric["FromState"], metric["ToState"], metric["ElapsedTime(s)"]))
           else:
               print ("Exiting metric parser")
               query_metric = False


if __name__ == '__main__':
   main()

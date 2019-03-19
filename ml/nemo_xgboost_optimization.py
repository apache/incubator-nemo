#!/usr/bin/python3
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
#

import numpy as np
import xgboost as xgb
import psycopg2 as pg
from sklearn import preprocessing
from pathlib import Path
import json, sys, getopt
# import matplotlib.pyplot as plt

# ########################################################
# METHODS
# ########################################################
def format_row(duration, inputsize, jvmmemsize, totalmemsize, vertex_properties, edge_properties):
  duration_in_10sec = int(duration) // 1000
  inputsize_in_10kb = int(inputsize) // 10240  # capable of expressing upto around 20TB with int range
  jvmmemsize_in_mb = int(jvmmemsize) // 1048576
  totalmemsize_in_mb = int(totalmemsize) // 1048576
  return f'{duration_in_10sec} 0:{inputsize_in_10kb} 1:{jvmmemsize_in_mb} 2:{totalmemsize_in_mb} {vertex_properties} {edge_properties}'

# ########################################################
def load_data_from_db(tablename):
  conn = None

  try:
    host = "nemo-optimization.cabbufr3evny.us-west-2.rds.amazonaws.com"
    dbname = "nemo_optimization"
    dbuser = "postgres"
    dbpwd = "fake_password"
    conn = pg.connect(host=host, dbname=dbname, user=dbuser, password=dbpwd)
  except:
    print("I am unable to connect to the database")

  sql = "SELECT * from " + tablename
  cur = conn.cursor()
  try:
    cur.execute(sql)
  except:
    print("I can't run " + sql)

  rows = cur.fetchall()
  processed_rows = [format_row(row[1], row[2], row[3], row[4], row[5], row[6]) for row in rows]
  cur.close()
  conn.close()
  return processed_rows

# ########################################################
def write_to_file(filename, rows):
  f = open(filename, 'w')
  for row in rows:
    f.write(row + "\n")
  f.close()

def encode_processed_rows(processed_rows, col_to_id):
  for i,row in enumerate(processed_rows):
    arr = row.split()
    for j,it in enumerate(arr[1:]):
      k,v = it.split(':')
      ek = col_to_id[int(k)]
      arr[j+1] = f'{ek}:{v}'
    processed_rows[i] = ' '.join(arr)
  return processed_rows

def decode_rows(rows, id_to_col):
  for i,row in enumerate(rows):
    arr = row.split()
    for j,it in enumerate(arr[1:]):
      ek,v = it.split(':')
      k = id_to_col[int(ek)]
      arr[j+1] = f'{k}:{v}'
    rows[i] = ' '.join(arr)
  return rows

# ########################################################
def stringify_num(num):
  return str(round(num, 2))

def dict_union(d1, d2):
  for k,v in d2.items():
    if k in d1:
      if type(d1[k]) is dict and type(v) is dict:
        d1[k] = dict_union(d1[k], v)
      else:
        d1[k] = d1[k] + v
    else:
      d1[k] = v
  return d1

# ########################################################
class Tree:
  root = None
  idx_to_node = {}

  def append_to_dict_if_not_exists(self, idx, node):
    if idx not in self.idx_to_node:
      self.idx_to_node[idx] = node

  def addNode(self, index, feature_id, split, yes, no, missing, value):
    n = None
    if self.root == None:
      self.root = Node(None)
      n = self.root
      self.append_to_dict_if_not_exists(index, n)
    else:
      n = self.idx_to_node[index]

    self.append_to_dict_if_not_exists(yes, Node(n))
    self.append_to_dict_if_not_exists(no, Node(n))
    self.append_to_dict_if_not_exists(missing, Node(n))
    n.addAttributes(index, feature_id, split, yes, no, missing, value, self.idx_to_node)

  def importanceDict(self):
    return self.root.importanceDict()

  def __str__(self):
    return json.dumps(json.loads(str(self.root)), indent=4)

class Node:
  parent = None
  index = None

  feature = None
  split = None
  left = None
  right = None
  missing = None

  value = None

  def __init__(self, parent):
    self.parent = parent

  def addAttributes(self, index, feature_id, split, yes, no, missing, value, idx_to_node):
    self.index = index
    if feature_id == 'Leaf':
      self.value = value
    else:
      self.feature = feature_id
      self.split = split
      self.left = idx_to_node[yes]
      self.right = idx_to_node[no]
      self.missing = idx_to_node[missing]

  def isLeaf(self):
    return self.value != None

  def isRoot(self):
    return self.parent == None

  def getIndex(self):
    return self.index

  def getLeft(self):
    return self.left

  def getRight(self):
    return self.right

  def getMissing(self):
    return self.missing

  def getApprox(self):
    if self.isLeaf():
      return self.value
    else:
      lapprox = self.left.getApprox()
      rapprox = self.right.getApprox()
      if rapprox != 0 and abs(lapprox/rapprox) < 0.04:  # smaller than 4% then ignore
        return rapprox
      elif lapprox != 0 and abs(rapprox/lapprox) < 0.04:
        return lapprox
      else:
        return (lapprox + rapprox) / 2

  def getDiff(self):
    lapprox = self.left.getApprox()
    rapprox = self.right.getApprox()
    if (rapprox != 0 and abs(lapprox/rapprox) < 0.04) or (lapprox != 0 and abs(rapprox/lapprox) < 0.04):
      return 0
    return lapprox - rapprox

  def importanceDict(self):
    if self.isLeaf():
      return {}
    else:
      d = {}
      d[self.feature] = {self.split: self.getDiff()}
      return dict_union(d, dict_union(self.left.importanceDict(), self.right.importanceDict()))

  def __str__(self):
    if self.isLeaf():
      return f'{stringify_num(self.value)}'
    else:
      left = str(self.left) if self.left.isLeaf() else json.loads(str(self.left))
      right = str(self.right) if self.right.isLeaf() else json.loads(str(self.right))
      return json.dumps({self.index: f'{self.feature}' + '{' + stringify_num(self.getApprox()) + ',' + stringify_num(self.getDiff()) + '}', 'L' + self.left.getIndex(): left, 'R' + self.right.getIndex(): right})

# ########################################################
# MAIN FUNCTION
# ########################################################
argv = sys.argv[1:]
tablename = ''
try:
  opts, args = getopt.getopt(argv,"ht:",["tablename="])
except getopt.GetoptError:
  print('nemo_xgboost_optimization.py -t <tablename>')
  sys.exit(2)
for opt, arg in opts:
  if opt == '-h':
    print('nemo_xgboost_optimization.py -t <tablename>')
    sys.exit()
  elif opt in ("-t", "--tablename"):
    tablename = arg

modelname = tablename + "_bst.model"
processed_rows = load_data_from_db(tablename)
# write_to_file('process_test', processed_rows)

## Make Dictionary
col = []
for row in processed_rows:
  arr = row.split()
  for it in arr[1:]:
    k,v = it.split(':')
    col.append(int(k))
le = preprocessing.LabelEncoder()
ids = le.fit_transform(col)
col_to_id = dict(zip(col, ids))
id_to_col = dict(zip(ids, col))

encoded_rows = encode_processed_rows(processed_rows, col_to_id)
write_to_file('nemo_optimization.out', encoded_rows)
# write_to_file('decode_test', decode_rows(encoded_rows, id_to_col))
ddata = xgb.DMatrix('nemo_optimization.out')

avg_20_duration = np.mean(ddata.get_label()[:20])
print("average job duration: ", avg_20_duration)
allowance = avg_20_duration // 25  # 4%

row_size = len(processed_rows)
print("total_rows: ", row_size)

## TRAIN
dtrain = ddata.slice([i for i in range(0, row_size) if i%6 != 5])  # mod is not 5
print("train_rows: ", dtrain.num_row())
dtest = ddata.slice([i for i in range(0, row_size) if i%6 == 5])  # mod is 5
print("test_rows: ", dtest.num_row())
labels = dtest.get_label()

## Existing booster
bst_opt = xgb.Booster(model_file=modelname) if Path(modelname).is_file() else None
preds_opt = bst_opt.predict(dtest) if bst_opt is not None else None
error_opt = (sum(1 for i in range(len(preds_opt)) if abs(preds_opt[i] - labels[i]) > allowance) / float(len(preds_opt))) if preds_opt is not None else 1
print('opt_error=%f' % error_opt)

learning_rates = [0.1, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9]
for lr in learning_rates:
  param = {'max_depth':6, 'eta':lr, 'verbosity':0, 'objective':'reg:linear'}

  watchlist = [(dtest, 'eval'), (dtrain, 'train')]
  num_round = row_size // 10
  bst = xgb.train(param, dtrain, num_round, watchlist, early_stopping_rounds=5)

  preds = bst.predict(dtest)
  error = (sum(1 for i in range(len(preds)) if abs(preds[i] - labels[i]) > allowance) / float(len(preds)))
  print('error=%f' % error)

  ## Better booster
  if error <= error_opt:
    bst_opt = bst
    bst.save_model(modelname)

## Let's now use bst_opt
# fscore = bst_opt.get_fscore()
# sorted_fscore = sorted(fscore.items(), key=lambda kv: kv[1])
# for i in range(len(sorted_fscore)):
#   print("\nSplit Value Histogram:")
#   feature = sorted_fscore.pop()[0]
#   print(feature, "=", id_to_col[int(feature[1:])])
#   hg = bst_opt.get_split_value_histogram(feature)
#   print(hg)

df = bst_opt.trees_to_dataframe()
# print("Trees to dataframe")
# print(df)

trees = {}
for index, row in df.iterrows():
  if row['Tree'] not in trees:
    trees[row['Tree']] = Tree()

  translated_feature = id_to_col[int(row['Feature'][1:])] if row['Feature'].startswith('f') else row['Feature']
  # print(translated_feature)
  trees[row['Tree']].addNode(row['ID'], translated_feature, row['Split'], row['Yes'], row['No'], row['Missing'], row['Gain'])

results = {}
print("\nGenerated Trees:")
for t in trees.values():
  results = dict_union(results, t.importanceDict())
  # print(t)

print("\nImportanceDict")
print(json.dumps(results, indent=2))

print("\nSummary")
resultsJson = []
for k,v in results.items():
  for kk,vv in v.items():
    resultsJson.append({'feature': k, 'split': kk, 'val': vv})
    how = 'greater' if vv > 0 else 'smaller'
    restring = f'{k} should be {how} than {kk}'
    print(restring)

with open("results.out", "w") as file:
  file.write(json.dumps(resultsJson, indent=2))

# Visualize tree
# xgb.plot_tree(bst_opt)
# plt.show()

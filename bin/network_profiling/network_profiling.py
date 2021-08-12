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
This script takes the slave nodes specified under ~/hadoop/etc/slaves, and measures the network information
between each pair of nodes specified with the hadoop slaves. This then aggregates each results, and writes them on
result.json, containing all network profile information for each pair of nodes. This runs in a multi-thread style.
'''

import paramiko
from tqdm import tqdm
import os, getpass, json, datetime
import threading


def collect_candidates(list):
  '''
  Collect candidate pairs of nodes from the list of slave nodes.
  :param list: the list of slave nodes.
  :return: candidate pairs of nodes, each joined by a slash (/).
  '''
  if len(list) == 1:
    return []
  else:
    src = list[0]
    dsts = list[1:]
    return ['{}/{}'.format(src, dst) for dst in dsts] + collect_candidates(dsts)


def qperf(candidate, username, key, busy_nodes, result):
  '''
  Measure the network statistics of the candidate nodes using the qperf library from linux.
  :param candidate: a pair of nodes, joined by a slash (/).
  :param username: the username to use to connect via ssh.
  :param key: the key to use to connect via ssh.
  :param busy_nodes: we keep the list of busy nodes that are currently being profiled to prevent intervention.
  :param result: we append the result to this result.
  :return: void.
  '''
  ssh = paramiko.SSHClient()
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

  hostname, hostname2 = candidate.split('/')

  ssh.connect(hostname, username=username, pkey=key)
  ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('qperf')

  ssh.connect(hostname2, username=username, pkey=key)
  ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('qperf -v -uu {} tcp_bw tcp_lat quit'.format(hostname))
  # print('analysis complete for connection {}'.format(candidate))

  lines = ssh_stdout.readlines()
  busy_nodes.remove(hostname)
  busy_nodes.remove(hostname2)
  """
  tcp_bw:
      bw              =  1.14 GB/sec
      msg_rate        =  17.3 K/sec
      send_cost       =   941 ms/GB
      recv_cost       =   322 ms/GB
      send_cpus_used  =   107 % cpus
      recv_cpus_used  =  36.5 % cpus
  tcp_lat:
      latency        =    54 us
      msg_rate       =  18.5 K/sec
      loc_cpus_used  =   178 % cpus
      rem_cpus_used  =    11 % cpus
  quit:
  """
  res = {}
  for line in lines:
    if line.startswith('    '):  # only the metrics, not the headers
      k, v = line.split('=')  # split key and value
      res[k.strip()] = v.strip()
  result[candidate] = res
  ssh.close()


slaves_path = os.path.join(os.environ["HOME"], "hadoop", "etc", "hadoop", "slaves")
with open(slaves_path, 'r') as fp:
  slaves = fp.read().splitlines()

candidates = collect_candidates(sorted(slaves))

busy_nodes = []
threads = []

path = os.path.join(os.environ["HOME"], ".ssh", "id_rsa")
key = paramiko.RSAKey.from_private_key_file(path)
username = getpass.getuser()
result = {}

print("Starting analysis:")

pbar = tqdm(total=len(candidates))
while candidates:
  candidate = candidates.pop()
  hostname, hostname2 = candidate.split('/')

  if hostname in busy_nodes or hostname2 in busy_nodes:
    candidates.insert(0, candidate)

  # OPEN CASES FOR OPTIMIZATION:
  # elif ...:

  else:
    busy_nodes.append(hostname)
    busy_nodes.append(hostname2)

    thr = threading.Thread(target=qperf, args=(candidate, username, key, busy_nodes, result), kwargs={})
    threads.append(thr)
    thr.start()
    pbar.update(1)
pbar.close()

for thr in threads:
  thr.join()

# print(result)
result['slaves'] = '/'.join(slaves)
result['timestamp'] = str(datetime.datetime.now())
with open('result.json', 'w') as fp:
  json.dump(result, fp)

# with open('../../../network_profiling/result_{}.json'.format(datetime.datetime.now()), 'w') as fp:
#   json.dump(result, fp)

#!/usr/bin/env python3
#
# Copyright (C) 2017 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

'''
json2dot.py: Generates Graphviz representation of Vortex DAG::toString
This file is used as backend for https://service.jangho.kr/vortex-dag
'''

import sys
import json
import re

nextIdx = 0

def getIdx():
    global nextIdx
    nextIdx += 1
    return nextIdx

class DAG:
    '''
    A class for converting DAG to Graphviz representation.
    JSON representation should be formatted like what toString method in DAG.java does.
    '''
    def __init__(self, dag):
        self.vertices = {}
        self.edges = []
        for vertex in dag['vertices']:
            self.vertices[vertex['id']] = Vertex(vertex['id'], vertex['properties'])
        for edge in dag['edges']:
            self.edges.append(Edge(self.vertices[edge['src']], self.vertices[edge['dst']], edge['properties']))
    @property
    def dot(self):
        dot = ''
        for vertex in self.vertices.values():
            dot += vertex.dot
        for edge in self.edges:
            dot += edge.dot
        return dot

def Vertex(id, properties):
    try:
        return PhysicalStage(id, properties)
    except:
        pass
    try:
        return Stage(id, properties)
    except:
        pass
    try:
        return Task(id, properties)
    except:
        pass
    return NormalVertex(id, properties)

class NormalVertex:
    def __init__(self, id, properties):
        self.id = id
        self.properties = properties
        self.idx = getIdx()
    @property
    def dot(self):
        color = 'black'
        try:
            if (self.properties['attributes']['Placement'] == 'Transient'):
                color = 'orange'
            if (self.properties['attributes']['Placement'] == 'Reserved'):
                color = 'green'
        except:
            pass
        label = self.id
        try:
            label += ' (p{})'.format(self.properties['attributes']['Parallelism'])
        except:
            pass
        try:
            label += '\\n{}'.format(self.properties['source'])
        except:
            pass
        try:
            label += '\\n{}'.format(self.properties['runtimeVertexId'])
        except:
            pass
        try:
            label += '\\n{}'.format(self.properties['index'])
        except:
            pass
        try:
            m = re.search('^([a-zA-Z_]*):([a-zA-Z_\.]*)', self.properties['transform'])
            label += '\\n{}:{}'.format(m.group(1), m.group(2).split('.')[-1])
        except:
            pass
        dot = '{} [label="{}", color={}];'.format(self.idx, label, color)
        return dot
    @property
    def oneVertex(self):
        return self
    @property
    def logicalEnd(self):
        return self.idx

class Stage:
    def __init__(self, id, properties):
        self.id = id
        self.internalDAG = DAG(properties['stageInternalDAG'])
        self.idx = getIdx()
    @property
    def dot(self):
        dot = ''
        dot += 'subgraph cluster_{} {{'.format(self.idx)
        dot += 'label = "{}";'.format(self.id)
        dot += 'color=blue;'
        dot += self.internalDAG.dot
        dot += '}'
        return dot
    @property
    def oneVertex(self):
        return next(iter(self.internalDAG.vertices.values()))
    @property
    def logicalEnd(self):
        return 'cluster_{}'.format(self.idx)

class TaskGroup:
    def __init__(self, properties):
        self.taskGroupId = properties['taskGroupId']
        self.dag = DAG(properties['taskDAG'])
        self.resourceType = properties['resourceType']
        self.idx = getIdx()
    @property
    def dot(self):
        color = 'black'
        if self.resourceType == 'Transient':
            color = 'orange'
        if self.resourceType == 'Reserved':
            color = 'green'
        dot = 'subgraph cluster_{} {{'.format(self.idx)
        dot += 'label = "{}";'.format(self.taskGroupId)
        dot += 'color={};'.format(color)
        dot += self.dag.dot
        dot += '}'
        return dot
    @property
    def logicalEnd(self):
        return 'cluster_{}'.format(self.idx)

class PhysicalStage:
    def __init__(self, id, properties):
        self.id = id
        self.taskGroups = [TaskGroup(x) for x in properties['taskGroupList']]
        self.idx = getIdx()
    @property
    def dot(self):
        dot = 'subgraph cluster_{} {{'.format(self.idx)
        dot += 'label = "{}";'.format(self.id)
        dot += 'color=red;'
        for taskGroup in self.taskGroups:
            dot += taskGroup.dot
        dot += '}'
        return dot
    @property
    def oneVertex(self):
        return next(iter(self.taskGroups[0].dag.vertices.values()))
    @property
    def logicalEnd(self):
        return 'cluster_{}'.format(self.idx)

def Edge(src, dst, properties):
    try:
        return PhysicalStageEdge(src, dst, properties)
    except:
        pass
    try:
        return StageEdge(src, dst, properties)
    except:
        pass
    try:
        return RuntimeEdge(src, dst, properties)
    except:
        pass
    try:
        return IREdge(src, dst, properties)
    except:
        pass
    return NormalEdge(src, dst, properties)

class NormalEdge:
    def __init__(self, src, dst, properties):
        self.src = src
        self.dst = dst
    @property
    def dot(self):
        return '{} -> {} [ltail = {}, lhead = {}];'.format(self.src.oneVertex.idx, self.dst.oneVertex.idx,
                self.src.logicalEnd, self.dst.logicalEnd)

class IREdge:
    def __init__(self, src, dst, properties):
        self.src = src
        self.dst = dst
        self.id = properties['id']
        self.attributes = properties['attributes']
    @property
    def dot(self):
        label = self.id + '\\n' + '/'.join(self.attributes.values())
        return '{} -> {} [ltail = {}, lhead = {}, label = "{}"];'.format(self.src.oneVertex.idx,
                self.dst.oneVertex.idx, self.src.logicalEnd, self.dst.logicalEnd, label)

class PhysicalStageEdge:
    def __init__(self, src, dst, properties):
        self.src = src
        self.dst = dst
        self.runtimeEdgeId = properties['runtimeEdgeId']
        self.runtimeEdge = properties['runtimeEdge']
        self.externalVertexAttr = properties['externalVertexAttr']
    @property
    def dot(self):
        color = 'black'
        try:
            if self.externalVertexAttr['ResourceType'] == 'Transient':
                color = 'orange'
            if self.externalVertexAttr['ResourceType'] == 'Reserved':
                color = 'green'
        except:
            pass
        label = self.runtimeEdgeId + ' (p{})\\n'.format(self.externalVertexAttr['Parallelism']) + '/'.join(self.runtimeEdge.values())
        return '{} -> {} [ltail = {}, lhead = {}, label = "{}", color = {}];'.format(self.src.oneVertex.idx,
                self.dst.oneVertex.idx, self.src.logicalEnd, self.dst.logicalEnd, label, color)

class StageEdge:
    def __init__(self, src, dst, properties):
        self.src = src.internalDAG.vertices[properties['srcRuntimeVertex']]
        self.dst = dst.internalDAG.vertices[properties['dstRuntimeVertex']]
        self.runtimeEdgeId = properties['runtimeEdgeId']
        self.runtimeEdge = properties['runtimeEdge']
    @property
    def dot(self):
        label = self.runtimeEdgeId + '\\n' + '/'.join(self.runtimeEdge.values())
        return '{} -> {} [ltail = {}, lhead = {}, label = "{}"];'.format(self.src.oneVertex.idx,
                self.dst.oneVertex.idx, self.src.logicalEnd, self.dst.logicalEnd, label)

class RuntimeEdge:
    def __init__(self, src, dst, properties):
        self.src = src
        self.dst = dst
        self.runtimeEdgeId = properties['runtimeEdgeId']
        self.runtimeEdge = properties['runtimeEdge']
    @property
    def dot(self):
        label = self.runtimeEdgeId + '\\n' + '/'.join(self.runtimeEdge.values())
        return '{} -> {} [ltail = {}, lhead = {}, label = "{}"];'.format(self.src.oneVertex.idx,
                self.dst.oneVertex.idx, self.src.logicalEnd, self.dst.logicalEnd, label)

def jsonToDot(dagJSON):
    return 'digraph dag {compound=true; nodesep=1.0; forcelabels=true;' + DAG(dagJSON).dot + '}'

if __name__ == "__main__":
    print(jsonToDot(json.loads(sys.stdin.read())))

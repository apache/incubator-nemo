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
This file is used as backend for https://service.jangho.io/onyx-dag/
'''

import sys
import json
import re

nextIdx = 0

def edgePropertiesString(properties):
    return '/'.join(['SideInput' if x[0] == 'IsSideInput' else x[1].split('.')[-1] for x in sorted(properties.items())])

def getIdx():
    global nextIdx
    nextIdx += 1
    return nextIdx

def stateToColor(state):
    try:
        return {'READY': '#fffbe2',
                'EXECUTING': '#e2fbff',
                'COMPLETE': '#e2ffe5',
                'FAILED_RECOVERABLE': '#ffe2e2',
                'FAILED_UNRECOVERABLE': '#e2e2e2'}[state]
    except:
        return 'white'

class JobState:
    def __init__(self, data):
        self.id = data['jobId']
        self.stages = {}
        for stage in data['physicalStages']:
            self.stages[stage['id']] = PhysicalStageState(stage)
    @classmethod
    def empty(cls):
        return cls({'jobId': None, 'physicalStages': []})
    def get(self, id):
        try:
            return self.stages[id]
        except:
            return PhysicalStageState.empty()

class PhysicalStageState:
    def __init__(self, data):
        self.id = data['id']
        self.state = data['state']
        self.taskGroups = {}
        for taskGroup in data['taskGroups']:
            self.taskGroups[taskGroup['id']] = TaskGroupState(taskGroup)
    @classmethod
    def empty(cls):
        return cls({'id': None, 'state': None, 'taskGroups': []})
    def get(self, id):
        try:
            return self.taskGroups[id]
        except:
            return TaskGroupState.empty()

class TaskGroupState:
    def __init__(self, data):
        self.id = data['id']
        self.state = data['state']
        self.tasks = {}
        for task in data['tasks']:
            self.tasks[task['id']] = TaskState(task)
    @classmethod
    def empty(cls):
        return cls({'id': None, 'state': None, 'tasks': []})
    def get(self, id):
        try:
            return self.tasks[id]
        except:
            return TaskState.empty()

class TaskState:
    def __init__(self, data):
        self.id = data['id']
        self.state = data['state']
    @classmethod
    def empty(cls):
        return cls({'id': None, 'state': None})

class DAG:
    '''
    A class for converting DAG to Graphviz representation.
    JSON representation should be formatted like what toString method in DAG.java does.
    '''
    def __init__(self, dag, jobState):
        self.vertices = {}
        self.edges = []
        for vertex in dag['vertices']:
            self.vertices[vertex['id']] = Vertex(vertex['id'], vertex['properties'], jobState.get(vertex['id']))
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

def Vertex(id, properties, state):
    try:
        return PhysicalStage(id, properties, state)
    except:
        pass
    try:
        return Stage(id, properties)
    except:
        pass
    try:
        return LoopVertex(id, properties)
    except:
        pass
    return NormalVertex(id, properties, state)

class NormalVertex:
    def __init__(self, id, properties, state):
        self.id = id
        self.properties = properties
        self.idx = getIdx()
        self.state = state.state
    @property
    def dot(self):
        color = 'black'
        try:
            if (self.properties['executionProperties']['ExecutorPlacement'] == 'Transient'):
                color = 'orange'
            if (self.properties['executionProperties']['ExecutorPlacement'] == 'Reserved'):
                color = 'green'
        except:
            pass
        label = self.id
        if self.state is not None:
            label += '\\n({})'.format(self.state)
        try:
            label += ' (p{})'.format(self.properties['executionProperties']['Parallelism'])
        except:
            pass
        try:
            label += ' (s{})'.format(self.properties['executionProperties']['ScheduleGroupIndex'])
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
        dot = '{} [label="{}", color={}, style=filled, fillcolor="{}"];'.format(self.idx, label, color, stateToColor(self.state))
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
        self.internalDAG = DAG(properties['stageInternalDAG'], JobState.empty())
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
        return next(iter(self.internalDAG.vertices.values())).oneVertex
    @property
    def logicalEnd(self):
        return 'cluster_{}'.format(self.idx)

class LoopVertex:
    def __init__(self, id, properties):
        self.id = id
        self.dag = DAG(properties['DAG'], JobState.empty())
        self.remaining_iteration = properties['remainingIteration']
        self.executionProperties = properties['executionProperties']
        self.incoming = properties['dagIncomingEdges']
        self.outgoing = properties['dagOutgoingEdges']
        self.edgeMapping = properties['edgeWithLoopToEdgeWithInternalVertex']
        self.idx = getIdx()
    @property
    def dot(self):
        label = self.id
        try:
            label += ' (p{})'.format(self.executionProperties['Parallelism'])
        except:
            pass
        label += '\\n(Remaining iteration: {})'.format(self.remaining_iteration)
        dot = 'subgraph cluster_{} {{'.format(self.idx)
        dot += 'label = "{}";'.format(label)
        dot += self.dag.dot
        dot += '}'
        return dot
    @property
    def oneVertex(self):
        return next(iter(self.dag.vertices.values())).oneVertex
    @property
    def logicalEnd(self):
        return 'cluster_{}'.format(self.idx)
    def internalSrcFor(self, edgeWithLoopId):
        edgeId = self.edgeMapping[edgeWithLoopId]
        vertexId = list(filter(lambda v: edgeId in self.outgoing[v], self.outgoing))[0]
        return self.dag.vertices[vertexId]
    def internalDstFor(self, edgeWithLoopId):
        edgeId = self.edgeMapping[edgeWithLoopId]
        vertexId = list(filter(lambda v: edgeId in self.incoming[v], self.incoming))[0]
        return self.dag.vertices[vertexId]

class TaskGroup:
    def __init__(self, properties, state):
        self.taskGroupId = properties['taskGroupId']
        self.taskGroupIdx = properties['taskGroupIdx']
        self.dag = DAG(properties['taskDAG'], state)
        self.containerType = properties['containerType']
        self.idx = getIdx()
        self.state = state.state
    @property
    def dot(self):
        color = 'black'
        if self.containerType == 'Transient':
            color = 'orange'
        if self.containerType == 'Reserved':
            color = 'green'
        if self.state is None:
            state = ''
        else:
            state = ' ({})'.format(self.state)
        dot = 'subgraph cluster_{} {{'.format(self.idx)
        dot += 'label = "{} ({}){}";'.format(self.taskGroupId, self.taskGroupIdx, state)
        dot += 'color={}; bgcolor="{}";'.format(color, stateToColor(self.state))
        dot += self.dag.dot
        dot += '}'
        return dot
    @property
    def logicalEnd(self):
        return 'cluster_{}'.format(self.idx)

class PhysicalStage:
    def __init__(self, id, properties, state):
        self.id = id
        self.taskGroups = [TaskGroup(x, state.get(x['taskGroupId'])) for x in properties['taskGroupList']]
        self.idx = getIdx()
        self.state = state.state
    @property
    def dot(self):
        if self.state is None:
            state = ''
        else:
            state = ' ({})'.format(self.state)
        dot = 'subgraph cluster_{} {{'.format(self.idx)
        dot += 'label = "{}{}";'.format(self.id, state)
        dot += 'color=red; bgcolor="{}";'.format(stateToColor(self.state))
        for taskGroup in self.taskGroups:
            dot += taskGroup.dot
        dot += '}'
        return dot
    @property
    def oneVertex(self):
        return next(iter(self.taskGroups[0].dag.vertices.values())).oneVertex
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
        self.executionProperties = properties['executionProperties']
        self.coder = properties['coder']
    @property
    def dot(self):
        src = self.src
        dst = self.dst
        try:
            src = src.internalSrcFor(self.id)
        except:
            pass
        try:
            dst = dst.internalDstFor(self.id)
        except:
            pass
        label = '{}<BR/>{}<BR/><FONT POINT-SIZE=\'10\'>{}</FONT>'.format(self.id, edgePropertiesString(self.executionProperties), self.coder)
        return '{} -> {} [ltail = {}, lhead = {}, label = <{}>];'.format(src.oneVertex.idx,
                dst.oneVertex.idx, src.logicalEnd, dst.logicalEnd, label)

class PhysicalStageEdge:
    def __init__(self, src, dst, properties):
        self.src = src
        self.dst = dst
        self.runtimeEdgeId = properties['runtimeEdgeId']
        self.edgeProperties = properties['edgeProperties']
        self.externalVertexAttr = properties['externalVertexAttr']
        self.parallelism = self.externalVertexAttr['Parallelism']
        self.coder = properties['coder']
    @property
    def dot(self):
        color = 'black'
        try:
            if self.externalVertexAttr['ContainerType'] == 'Transient':
                color = 'orange'
            if self.externalVertexAttr['ContainerType'] == 'Reserved':
                color = 'green'
        except:
            pass
        label = '{} (p{})<BR/>{}<BR/><FONT POINT-SIZE=\'10\'>{}</FONT>'.format(self.runtimeEdgeId, self.parallelism, edgePropertiesString(self.edgeProperties), self.coder)
        return '{} -> {} [ltail = {}, lhead = {}, label = <{}>, color = {}];'.format(self.src.oneVertex.idx,
                self.dst.oneVertex.idx, self.src.logicalEnd, self.dst.logicalEnd, label, color)

class StageEdge:
    def __init__(self, src, dst, properties):
        self.src = src.internalDAG.vertices[properties['srcVertex']]
        self.dst = dst.internalDAG.vertices[properties['dstVertex']]
        self.runtimeEdgeId = properties['runtimeEdgeId']
        self.edgeProperties = properties['edgeProperties']
        self.coder = properties['coder']
    @property
    def dot(self):
        label = '{}<BR/>{}<BR/><FONT POINT-SIZE=\'10\'>{}</FONT>'.format(self.runtimeEdgeId, edgePropertiesString(self.edgeProperties), self.coder)
        return '{} -> {} [ltail = {}, lhead = {}, label = <{}>];'.format(self.src.oneVertex.idx,
                self.dst.oneVertex.idx, self.src.logicalEnd, self.dst.logicalEnd, label)

class RuntimeEdge:
    def __init__(self, src, dst, properties):
        self.src = src
        self.dst = dst
        self.runtimeEdgeId = properties['runtimeEdgeId']
        self.edgeProperties = properties['edgeProperties']
        self.coder = properties['coder']
    @property
    def dot(self):
        label = '{}<BR/>{}<BR/><FONT POINT-SIZE=\'10\'>{}</FONT>'.format(self.runtimeEdgeId, edgePropertiesString(self.edgeProperties), self.coder)
        return '{} -> {} [ltail = {}, lhead = {}, label = <{}>];'.format(self.src.oneVertex.idx,
                self.dst.oneVertex.idx, self.src.logicalEnd, self.dst.logicalEnd, label)

def jsonToDot(jsonDict):
    try:
        dag = DAG(jsonDict['dag'], JobState(jsonDict['jobState']))
    except:
        dag = DAG(jsonDict, JobState.empty())
    return 'digraph dag {compound=true; nodesep=1.0; forcelabels=true;' + dag.dot + '}'

if __name__ == "__main__":
    print(jsonToDot(json.loads(sys.stdin.read())))

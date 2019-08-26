/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master.resource;

import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

/**
 * TaskSerializer.
 */
public class TaskSerializer {

  public TaskSerializer(Task task) {
  }

  public byte[] getSerialized(Task task) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    int offsetPlanId = 0;
    int offsetTaskId, offsetIncomingEdges, offsetOutgoingEdges,
      offsetExecutionProperty, offsetSerializedIRDag;

    // planId, taskId
    byte[] bytes = task.getPlanId().getBytes();
    bos.write(bytes, offsetPlanId, bytes.length);
    offsetTaskId = offsetPlanId + bytes.length;
    bytes = task.getTaskId().getBytes();
    bos.write(bytes, offsetTaskId, bytes.length);
    offsetIncomingEdges = offsetTaskId + bytes.length;

    // taskIncomingEdges, taskOutgoingEdges
    List<StageEdge> taskIncomingEdges = task.getTaskIncomingEdges();
    for(StageEdge stageEdge : taskIncomingEdges) {
      bytes = this.serializeStageEdge(stageEdge);
      assert(bytes != null);
      bos.write(bytes, offset, bytes.length);
      offset += bytes.length;
    }
    List<StageEdge> taskOutgoingEdges = task.getTaskOutgoingEdges();
    for(StageEdge stageEdge : taskOutgoingEdges) {
      bytes = this.serializeStageEdge(stageEdge);
      bos.write(bytes, offset, bytes.length);
      offset += bytes.length;
    }

    // executionProperty
    ExecutionPropertyMap<VertexExecutionProperty> executionProperty = task.getExecutionProperties();
    bytes = this.serializeExecutionProperty(executionProperty);
    assert(bytes != null);
    bos.write(bytes, offset, bytes.length);
    offset += bytes.length;

    // serializedIRDag
    bytes = task.getSerializedIRDag();
    assert(bytes != null);
    bos.write(bytes, offset, bytes.length);
    offset += bytes.length;

    // irVertexIdToReadable
    bytes = this.serializeIrVertexIdToReadable(task.getIrVertexIdToReadable());
    assert(bytes != null);
    bos.write(bytes, offset, bytes.length);
    offset += bytes.length;

    return bos.toByteArray();
  }

  public Task deserialize(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream();
    return null;
  }

  private byte[] serializeStageEdge(StageEdge stageEdge) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(stageEdge);
      return bos.toByteArray();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private byte[] serializeExecutionProperty(ExecutionPropertyMap<VertexExecutionProperty> executionProperty) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(executionProperty);
      return bos.toByteArray();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private byte[] serializeIrVertexIdToReadable(Map<String, Readable> irVertexToReadable) {
   try {
     ByteArrayOutputStream bos = new ByteArrayOutputStream();
     ObjectOutputStream oos = new ObjectOutputStream(bos);
     oos.writeObject(irVertexToReadable);
     return bos.toByteArray();
   } catch (Exception e) {
     e.printStackTrace();
     return null;
   }
  }
}

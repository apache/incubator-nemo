/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.compiler.ir.executionproperty;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataFlowModelProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ParallelismProperty;
import edu.snu.vortex.compiler.optimizer.examples.EmptyComponents;
import edu.snu.vortex.runtime.executor.data.MemoryStore;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.OneToOne;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Hash;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test {@link ExecutionPropertyMap}.
 */
public class ExecutionPropertyMapTest {
  private final IRVertex source = new BoundedSourceVertex<>(new EmptyComponents.EmptyBoundedSource("Source"));
  private final IRVertex destination = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final IREdge edge = new IREdge(OneToOne.class, source, destination, Coder.DUMMY_CODER);

  private ExecutionPropertyMap edgeMap;
  private ExecutionPropertyMap vertexMap;

  @Before
  public void setUp() {
    this.edgeMap = ExecutionPropertyMap.of(edge, OneToOne.class);
    this.vertexMap = ExecutionPropertyMap.of(source);
  }

  @Test
  public void testDefaultValues() {
    assertEquals(Hash.class, edgeMap.get(ExecutionProperty.Key.Partitioning));
    assertEquals(1, vertexMap.<Integer>get(ExecutionProperty.Key.Parallelism).longValue());
    assertEquals(edge.getId(), edgeMap.getId());
    assertEquals(source.getId(), vertexMap.getId());
  }

  @Test
  public void testPutGetAndRemove() {
    edgeMap.put(DataStoreProperty.of(MemoryStore.class));
    assertEquals(MemoryStore.class, edgeMap.get(ExecutionProperty.Key.DataStore));
    edgeMap.put(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
    assertEquals(DataFlowModelProperty.Value.Pull, edgeMap.get(ExecutionProperty.Key.DataFlowModel));

    edgeMap.remove(ExecutionProperty.Key.DataFlowModel);
    assertNull(edgeMap.get(ExecutionProperty.Key.DataFlowModel));

    vertexMap.put(ParallelismProperty.of(100));
    assertEquals(100, vertexMap.<Integer>get(ExecutionProperty.Key.Parallelism).longValue());
  }
}

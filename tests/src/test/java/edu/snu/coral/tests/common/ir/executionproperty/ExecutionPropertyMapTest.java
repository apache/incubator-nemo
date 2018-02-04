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
package edu.snu.coral.tests.common.ir.executionproperty;

import edu.snu.coral.common.coder.Coder;
import edu.snu.coral.common.ir.edge.IREdge;
import edu.snu.coral.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.coral.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.coral.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;
import edu.snu.coral.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.coral.common.ir.vertex.IRVertex;
import edu.snu.coral.common.ir.vertex.OperatorVertex;
import edu.snu.coral.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.coral.compiler.optimizer.examples.EmptyComponents;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test {@link ExecutionPropertyMap}.
 */
public class ExecutionPropertyMapTest {
  private final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
  private final IRVertex destination = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final DataCommunicationPatternProperty.Value comPattern = DataCommunicationPatternProperty.Value.OneToOne;
  private final IREdge edge = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
      source, destination, Coder.DUMMY_CODER);

  private ExecutionPropertyMap edgeMap;
  private ExecutionPropertyMap vertexMap;

  @Before
  public void setUp() {
    this.edgeMap = ExecutionPropertyMap.of(edge, DataCommunicationPatternProperty.Value.OneToOne);
    this.vertexMap = ExecutionPropertyMap.of(source);
  }

  @Test
  public void testDefaultValues() {
    assertEquals(comPattern, edgeMap.get(ExecutionProperty.Key.DataCommunicationPattern));
    assertEquals(1, vertexMap.<Integer>get(ExecutionProperty.Key.Parallelism).longValue());
    assertEquals(edge.getId(), edgeMap.getId());
    assertEquals(source.getId(), vertexMap.getId());
  }

  @Test
  public void testPutGetAndRemove() {
    edgeMap.put(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    assertEquals(DataStoreProperty.Value.MemoryStore, edgeMap.get(ExecutionProperty.Key.DataStore));
    edgeMap.put(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
    assertEquals(DataFlowModelProperty.Value.Pull, edgeMap.get(ExecutionProperty.Key.DataFlowModel));

    edgeMap.remove(ExecutionProperty.Key.DataFlowModel);
    assertNull(edgeMap.get(ExecutionProperty.Key.DataFlowModel));

    vertexMap.put(ParallelismProperty.of(100));
    assertEquals(100, vertexMap.<Integer>get(ExecutionProperty.Key.Parallelism).longValue());
  }
}

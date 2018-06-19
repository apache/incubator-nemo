/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.common.ir.executionproperty;

import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.common.coder.EncoderFactory;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.test.EmptyComponents;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test {@link ExecutionPropertyMap}.
 */
public class ExecutionPropertyMapTest {
  private final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
  private final IRVertex destination = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final DataCommunicationPatternProperty.Value comPattern = DataCommunicationPatternProperty.Value.OneToOne;
  private final IREdge edge = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, source, destination);

  private ExecutionPropertyMap<EdgeExecutionProperty> edgeMap;
  private ExecutionPropertyMap<VertexExecutionProperty> vertexMap;

  @Before
  public void setUp() {
    this.edgeMap = ExecutionPropertyMap.of(edge, DataCommunicationPatternProperty.Value.OneToOne);
    this.vertexMap = ExecutionPropertyMap.of(source);
  }

  @Test
  public void testDefaultValues() {
    assertEquals(comPattern, edgeMap.get(DataCommunicationPatternProperty.class).get());
    assertEquals(1, vertexMap.get(ParallelismProperty.class).get().longValue());
    assertEquals(edge.getId(), edgeMap.getId());
    assertEquals(source.getId(), vertexMap.getId());
  }

  @Test
  public void testPutGetAndRemove() {
    edgeMap.put(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    assertEquals(DataStoreProperty.Value.MemoryStore, edgeMap.get(DataStoreProperty.class).get());
    edgeMap.put(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
    assertEquals(DataFlowModelProperty.Value.Pull, edgeMap.get(DataFlowModelProperty.class).get());
    edgeMap.put(EncoderProperty.of(EncoderFactory.DUMMY_ENCODER_FACTORY));
    assertEquals(EncoderFactory.DUMMY_ENCODER_FACTORY, edgeMap.get(EncoderProperty.class).get());
    edgeMap.put(DecoderProperty.of(DecoderFactory.DUMMY_DECODER_FACTORY));
    assertEquals(DecoderFactory.DUMMY_DECODER_FACTORY, edgeMap.get(DecoderProperty.class).get());

    edgeMap.remove(DataFlowModelProperty.class);
    assertFalse(edgeMap.get(DataFlowModelProperty.class).isPresent());

    vertexMap.put(ParallelismProperty.of(100));
    assertEquals(100, vertexMap.get(ParallelismProperty.class).get().longValue());
  }
}

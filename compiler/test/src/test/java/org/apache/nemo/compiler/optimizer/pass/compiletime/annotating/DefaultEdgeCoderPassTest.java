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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link DefaultEdgeEncoderPass} and {@link DefaultEdgeDecoderPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DefaultEdgeCoderPassTest {
  private IRDAG compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileWordCountDAG();
  }

  @Test
  public void testAnnotatingPass() {
    final AnnotatingPass encoderPass = new DefaultEdgeEncoderPass();
    assertTrue(encoderPass.getExecutionPropertiesToAnnotate().contains(EncoderProperty.class));
    final AnnotatingPass decoderPass = new DefaultEdgeDecoderPass();
    assertTrue(decoderPass.getExecutionPropertiesToAnnotate().contains(DecoderProperty.class));
  }

  @Test
  public void testNotOverride() {
    // Get the first coder from the compiled DAG
    final IREdge irEdge = compiledDAG.getOutgoingEdgesOf(compiledDAG.getTopologicalSort().get(0)).get(0);
    final EncoderFactory compiledEncoderFactory = irEdge.getPropertyValue(EncoderProperty.class).get();
    final DecoderFactory compiledDecoderFactory = irEdge.getPropertyValue(DecoderProperty.class).get();
    IRDAG processedDAG = new DefaultEdgeEncoderPass().apply(compiledDAG);
    processedDAG = new DefaultEdgeDecoderPass().apply(processedDAG);

    // Get the first coder from the processed DAG
    final IREdge processedIREdge = processedDAG.getOutgoingEdgesOf(processedDAG.getTopologicalSort().get(0)).get(0);
    final EncoderFactory processedEncoderFactory = processedIREdge.getPropertyValue(EncoderProperty.class).get();
    assertEquals(compiledEncoderFactory, processedEncoderFactory); // It must not be changed.
    final DecoderFactory processedDecoderFactory = processedIREdge.getPropertyValue(DecoderProperty.class).get();
    assertEquals(compiledDecoderFactory, processedDecoderFactory); // It must not be changed.
  }

  @Test
  public void testSetToDefault() throws Exception {
    // Remove the first coder from the compiled DAG (to let our pass to set as default coder).
    final IREdge irEdge = compiledDAG.getOutgoingEdgesOf(compiledDAG.getTopologicalSort().get(0)).get(0);
    irEdge.getExecutionProperties().remove(EncoderProperty.class);
    irEdge.getExecutionProperties().remove(DecoderProperty.class);
    IRDAG processedDAG = new DefaultEdgeEncoderPass().apply(compiledDAG);
    processedDAG = new DefaultEdgeDecoderPass().apply(processedDAG);

    // Check whether the pass set the empty coder to our default encoder & decoder.
    final IREdge processedIREdge = processedDAG.getOutgoingEdgesOf(processedDAG.getTopologicalSort().get(0)).get(0);
    final EncoderFactory processedEncoderFactory = processedIREdge.getPropertyValue(EncoderProperty.class).get();
    final DecoderFactory processedDecoderFactory = processedIREdge.getPropertyValue(DecoderProperty.class).get();
    assertEquals(EncoderFactory.DUMMY_ENCODER_FACTORY, processedEncoderFactory);
    assertEquals(DecoderFactory.DUMMY_DECODER_FACTORY, processedDecoderFactory);
  }
}

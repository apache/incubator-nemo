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
package org.apache.nemo.common.ir;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.*;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Tests for {@link IRDAG}.
 */
public class IRDAGTest {
  private static final int NUM_OF_RANDOM_SEEDS = 100;
  private static final int NUM_OF_METHOD_CALLS = 100;

  @Test
  public void testParallelism() {
    // simple test case

    // comm pattern

    // xxx
  }

  @Test
  public void testXX() {
    // simple test case
  }

  @Test
  public void testMessageBarrierVertex() {
    // simple test case
    MessageBarrierVertex;
    StreamVertex;
    SamplingVertex;
  }

  @Test
  public void testStreamVertex() {
    // simple test case
    // insert
    // delete
    MessageBarrierVertex;
    StreamVertex;
    SamplingVertex;
  }

  @Test
  public void testSamplingVertex() {
    // simple test case
    // insert
    // delete
  }

  @Test
  public void testRandomCalls() {
    IntStream.range(0, 100).boxed().forEach(seed -> {
      final Random random = new Random(seed);
      // (1) Randomly insert vertices
      // (2) Randomly annotate user-configurable properties
      // (3) Randomly delete vertices
      //
      // Actually execute...?
      //
      // user-configurable execution properties
      // Checker checks this...
      // IRDAGBuilder(?)
    });
  }



}

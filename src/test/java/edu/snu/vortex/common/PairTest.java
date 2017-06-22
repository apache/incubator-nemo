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
package edu.snu.vortex.common;

import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.IRVertex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link Pair}.
 */
public class PairTest {
  final Object leftObject = new Object();
  final Object rightObject = new Object();
  private final IRVertex leftSource = new BoundedSourceVertex<>(new TestUtil.EmptyBoundedSource("leftSource"));
  private final IRVertex rightSource = new BoundedSourceVertex<>(new TestUtil.EmptyBoundedSource("rightSource"));

  @Test
  public void testPair() {
    final Pair<Object, Object> objectPair = Pair.of(leftObject, rightObject);
    final Pair<Object, Object> identicalObjectPair = Pair.of(leftObject, rightObject);
    final Pair<IRVertex, IRVertex> irVertexPair = Pair.of(leftSource, rightSource);
    final Pair<Object, IRVertex> mixedPair = Pair.of(leftObject, rightSource);

    assertEquals(leftObject, objectPair.left());
    assertEquals(rightObject, objectPair.right());
    assertEquals(leftSource, irVertexPair.left());
    assertEquals(rightSource, irVertexPair.right());
    assertEquals(leftObject, mixedPair.left());
    assertEquals(rightSource, mixedPair.right());
    assertEquals(objectPair, identicalObjectPair);
  }
}

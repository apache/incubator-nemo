package org.apache.nemo.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link Pair}.
 */
public class PairTest {
  final Object leftObject = new Object();
  final Object rightObject = new Object();
  private final IRVertex leftSource = new EmptyComponents.EmptySourceVertex<>("leftSource");
  private final IRVertex rightSource = new EmptyComponents.EmptySourceVertex<>("rightSource");

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

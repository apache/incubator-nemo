package org.apache.nemo.common.ir.vertex;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.ir.vertex.transform.Transform;

/**
 * IRVertex that transforms input data.
 * It is to be constructed in the compiler frontend with language-specific data transform logic.
 */
public final class OperatorVertex extends IRVertex {
  private final Transform transform;

  /**
   * Constructor of OperatorVertex.
   * @param t transform for the OperatorVertex.
   */
  public OperatorVertex(final Transform t) {
    super();
    this.transform = t;
  }

  /**
   * Copy Constructor of OperatorVertex.
   * @param that the source object for copying
   */
  public OperatorVertex(final OperatorVertex that) {
    super();
    this.transform = that.transform;
  }

  @Override
  public OperatorVertex getClone() {
    return new OperatorVertex(this);
  }

  /**
   * @return the transform in the OperatorVertex.
   */
  public Transform getTransform() {
    return transform;
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("transform", transform.toString());
    return node;
  }
}

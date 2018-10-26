package org.apache.nemo.common.dag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.io.Serializable;

/**
 * A vertex in DAG.
 */
public abstract class Vertex implements Serializable {
  private final String id;

  /**
   * @param id unique identifier of the vertex
   */
  public Vertex(final String id) {
    this.id = id;
  }

  /**
   * @return identifier of the vertex
   */
  public final String getId() {
    return id;
  }
  /**
   * @return the numeric id of the vertex.
   */
  public final Integer getNumericId() {
    return Integer.parseInt(id.replaceAll("[^\\d.]", ""));
  }

  /**
   * @return JSON representation of additional properties
   */
  @SuppressWarnings("checkstyle:designforextension")
  public JsonNode getPropertiesAsJsonNode() {
    return JsonNodeFactory.instance.objectNode();
  }
}

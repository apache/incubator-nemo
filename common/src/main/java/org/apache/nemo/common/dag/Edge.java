package org.apache.nemo.common.dag;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects two vertices of a DAG.
 * This class can be extended for various DAG representations.
 *
 * @param <V> the vertex type.
 */
public class Edge<V extends Vertex> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Edge.class.getName());

  private final String id;
  private final V src;
  private final V dst;

  /**
   * Constructor for Edge.
   * @param id ID of the edge.
   * @param src source vertex.
   * @param dst destination vertex.
   */
  public Edge(final String id, final V src, final V dst) {
    this.id = id;
    this.src = src;
    this.dst = dst;
  }

  /**
   * @return the ID of the edge.
   */
  public final String getId() {
    return id;
  }

  /**
   * @return the numeric ID of the edge. (for edge id "edge-2", this method returns 2)
   */
  public final Integer getNumericId() {
    return Integer.parseInt(id.replaceAll("[^\\d.]", ""));
  }

  /**
   * @return source vertex.
   */
  public final V getSrc() {
    return src;
  }

  /**
   * @return destination vertex.
   */
  public final V getDst() {
    return dst;
  }

  /**
   * @return JSON representation of additional properties
   */
  @SuppressWarnings("checkstyle:designforextension")
  public JsonNode getPropertiesAsJsonNode() {
    return JsonNodeFactory.instance.objectNode();
  }
}

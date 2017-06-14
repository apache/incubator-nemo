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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.runtime.exception.UnsupportedAttributeException;
import edu.snu.vortex.utils.dag.Edge;

/**
 * Physical execution plan of intermediate data movement.
 */
public final class IREdge extends Edge<IRVertex> {
  /**
   * Type of edges.
   */
  public enum Type {
    OneToOne,
    Broadcast,
    ScatterGather,
  }

  private final AttributeMap attributes;
  private final Type type;
  private final Coder coder;

  /**
   * Constructor of IREdge.
   * @param type type of the edge.
   * @param src source vertex.
   * @param dst destination vertex.
   * @param coder coder.
   */
  public IREdge(final Type type,
                final IRVertex src,
                final IRVertex dst,
                final Coder coder) {
    super(IdManager.newEdgeId(), src, dst);
    this.attributes = AttributeMap.of(this);
    this.type = type;
    this.coder = coder;
    switch (this.getType()) {
      case OneToOne:
        setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);
        break;
      case Broadcast:
        setAttr(Attribute.Key.CommunicationPattern, Attribute.Broadcast);
        break;
      case ScatterGather:
        setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
        break;
      default:
        throw new UnsupportedAttributeException("There is no such edge type as: " + this.getType());
    }
  }

  /**
   * Set an attribute to the IREdge.
   * @param key key of the attribute.
   * @param val value of the attribute.
   * @return the IREdge with the attribute applied.
   */
  public IREdge setAttr(final Attribute.Key key, final Attribute val) {
    attributes.put(key, val);
    return this;
  }

  /**
   * Get the attribute of the IREdge.
   * @param key key of the attribute.
   * @return the attribute.
   */
  public Attribute getAttr(final Attribute.Key key) {
    return attributes.get(key);
  }

  /**
   * @return the AttributeMap of the IREdge.
   */
  public AttributeMap getAttributes() {
    return attributes;
  }

  /**
   * @return type of the edge.
   */
  public Type getType() {
    return type;
  }

  /**
   * @return coder for the edge.
   */
  public Coder getCoder() {
    return coder;
  }

  /**
   * @param edge edge to compare.
   * @return whether or not the edge has the same itinerary
   */
  public Boolean hasSameItineraryAs(final IREdge edge) {
    return getSrc().equals(edge.getSrc()) && getDst().equals(edge.getDst());
  }

  /**
   * Static function to copy attributes from an edge to the other.
   * @param fromEdge the edge to copy attributes from.
   * @param toEdge the edge to copy attributes to.
   */
  public static void copyAttributes(final IREdge fromEdge, final IREdge toEdge) {
    fromEdge.getAttributes().forEachAttr(toEdge::setAttr);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IREdge irEdge = (IREdge) o;

    return type.equals(irEdge.getType()) && hasSameItineraryAs(irEdge);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + getSrc().hashCode();
    result = 31 * result + getDst().hashCode();
    return result;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"id\": \"").append(getId());
    sb.append("\", \"attributes\": ").append(attributes);
    sb.append(", \"type\": \"").append(type);
    sb.append("\", \"coder\": \"").append(coder.toString());
    sb.append("\"}");
    return sb.toString();
  }
}

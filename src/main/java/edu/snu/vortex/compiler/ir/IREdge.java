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
    ScatterGather,
    Broadcast,
    OneToOne,
  }

  private final String id;
  private final AttributeMap attributes;
  private final Type type;

  public IREdge(final Type type,
         final IRVertex src,
         final IRVertex dst) {
    super(src, dst);
    this.id = IdManager.newEdgeId();
    this.attributes = AttributeMap.of(this);
    this.type = type;
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

  public String getId() {
    return id;
  }

  public IREdge setAttr(final Attribute.Key key, final Attribute val) {
    attributes.put(key, val);
    return this;
  }

  public Attribute getAttr(final Attribute.Key key) {
    return attributes.get(key);
  }

  public AttributeMap getAttributes() {
    return attributes;
  }

  public Type getType() {
    return type;
  }

  public IRVertex getSrcIRVertex() {
    return super.getSrc();
  }

  public IRVertex getDstIRVertex() {
    return super.getDst();
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

    if (type != irEdge.type) {
      return false;
    }
    if (!getSrc().equals(irEdge.getSrc())) {
      return false;
    }
    return getDst().equals(irEdge.getDst());
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + getSrc().hashCode();
    result = 31 * result + getDst().hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id);
    sb.append(", src: ");
    sb.append(getSrc().getId());
    sb.append(", dst: ");
    sb.append(getDst().getId());
    sb.append(", attributes: ");
    sb.append(attributes);
    sb.append(", type: ");
    sb.append(type);
    return sb.toString();
  }
}

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

import edu.snu.vortex.compiler.ir.operator.Operator;

import java.util.HashMap;

/**
 * Physical execution plan of intermediate data movement.
 * @param <I> input operator type.
 * @param <O> output operator type.
 */
public final class Edge<I, O> {
  /**
   * Type of edges.
   */
  public enum Type {
    ScatterGather,
    Broadcast,
    OneToOne,
  }

  private final String id;
  private final HashMap<Attributes.Key, Attributes.Val> attributes;
  private final Type type;
  private final Operator<?, I> src;
  private final Operator<O, ?> dst;

  Edge(final Type type,
       final Operator<?, I> src,
       final Operator<O, ?> dst) {
    this.id = IdManager.newEdgeId();
    this.attributes = new HashMap<>(0);
    this.type = type;
    this.src = src;
    this.dst = dst;
  }

  public String getId() {
    return id;
  }

  public Edge<I, O> setAttr(final Attributes.Key key, final Attributes.Val val) {
    attributes.put(key, val);
    return this;
  }

  public Attributes.Val getAttr(final Attributes.Key key) {
    return attributes.get(key);
  }

  public Type getType() {
    return type;
  }

  public Operator<?, I> getSrc() {
    return src;
  }

  public Operator<O, ?> getDst() {
    return dst;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Edge<?, ?> edge = (Edge<?, ?>) o;

    if (type != edge.type) {
      return false;
    }
    if (!src.equals(edge.src)) {
      return false;
    }
    return dst.equals(edge.dst);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + src.hashCode();
    result = 31 * result + dst.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id);
    sb.append(", src: ");
    sb.append(src.getId());
    sb.append(", dst: ");
    sb.append(dst.getId());
    sb.append(", attributes: ");
    sb.append(attributes);
    sb.append(", type: ");
    sb.append(type);
    return sb.toString();
  }
}

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
import edu.snu.vortex.utils.dag.Vertex;

/**
 * The top-most wrapper for a user operation in the Vortex IR.
 */
public abstract class IRVertex extends Vertex {
  private final AttributeMap attributes;

  /**
   * Constructor of IRVertex.
   */
  public IRVertex() {
    super(IdManager.newVertexId());
    this.attributes = AttributeMap.of(this);
  }

  /**
   * @return a clone elemnt of the IRVertex.
   */
  public abstract IRVertex getClone();

  /**
   * Set an attribute to the IRVertex.
   * @param key key of the attribute.
   * @param val value of the attribute.
   * @return the IRVertex with the attribute applied.
   */
  public final IRVertex setAttr(final Attribute.Key key, final Attribute val) {
    attributes.put(key, val);
    return this;
  }
  /**
   * Set an attribute to the IRVertex.
   * @param key key of the attribute.
   * @param val value of the attribute.
   * @return the IRVertex with the attribute applied.
   */
  public final IRVertex setAttr(final Attribute.IntegerKey key, final Integer val) {
    attributes.put(key, val);
    return this;
  }

  /**
   * Get the attribute of the IRVertex.
   * @param key key of the attribute.
   * @return the attribute.
   */
  public final Attribute getAttr(final Attribute.Key key) {
    return attributes.get(key);
  }
  /**
   * Get the attribute of the IRVertex.
   * @param key key of the attribute.
   * @return the integer attribute.
   */
  public final Integer getAttr(final Attribute.IntegerKey key) {
    return attributes.get(key);
  }

  /**
   * @return the AttributeMap of the IRVertex.
   */
  public final AttributeMap getAttributes() {
    return attributes;
  }

  /**
   * @return IRVertex properties in String form.
   */
  protected final String irVertexPropertiesToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\"class\": \"").append(this.getClass().getSimpleName());
    sb.append("\", \"attributes\": ").append(attributes);
    return sb.toString();
  }
}

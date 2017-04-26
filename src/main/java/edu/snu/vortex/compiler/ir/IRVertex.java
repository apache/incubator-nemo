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

import java.io.Serializable;

/**
 * The top-most wrapper for a user operation in the Vortex IR.
 */
public abstract class IRVertex extends Vertex implements Serializable {
  private final AttributeMap attributes;

  public IRVertex() {
    super(IdManager.newOperatorId());
    this.attributes = AttributeMap.of(this);
  }

  public final IRVertex setAttr(final Attribute.Key key, final Attribute val) {
    attributes.put(key, val);
    return this;
  }
  public final IRVertex setAttr(final Attribute.IntegerKey key, final Integer val) {
    attributes.put(key, val);
    return this;
  }

  public final Attribute getAttr(final Attribute.Key key) {
    return attributes.get(key);
  }
  public final Integer getAttr(final Attribute.IntegerKey key) {
    return attributes.get(key);
  }

  public final AttributeMap getAttributes() {
    return attributes;
  }

  protected final String irVertexPropertiesToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\"class\": \"").append(this.getClass().getSimpleName());
    sb.append("\", \"attributes\": ").append(attributes);
    return sb.toString();
  }
}

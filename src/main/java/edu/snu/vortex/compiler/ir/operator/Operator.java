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
package edu.snu.vortex.compiler.ir.operator;

import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.IdManager;
import edu.snu.vortex.compiler.ir.util.AttributesMap;

import java.io.Serializable;

/**
 * Physical execution plan of a user operator.
 * @param <I> input type.
 * @param <O> output type.
 */
public abstract class Operator<I, O> implements Serializable {
  private final String id;
  private final AttributesMap attributes;

  public Operator() {
    this.id = IdManager.newOperatorId();
    this.attributes = new AttributesMap();
  }

  public final String getId() {
    return id;
  }

  public final Operator<I, O> setAttr(final Attributes.Key key, final Attributes val) {
    attributes.put(key, val);
    return this;
  }

  public final Attributes getAttr(final Attributes.Key key) {
    return attributes.get(key);
  }

  public final AttributesMap getAttributes() {
    return attributes;
  }

  @SuppressWarnings("checkstyle:designforextension")
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("class: ");
    sb.append(this.getClass().getSimpleName());
    sb.append(", id: ");
    sb.append(id);
    sb.append(", attributes: ");
    sb.append(attributes);
    return sb.toString();
  }
}

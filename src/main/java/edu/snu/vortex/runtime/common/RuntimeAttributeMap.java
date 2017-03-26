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
package edu.snu.vortex.runtime.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Data structure for managing {@link RuntimeAttribute} for each Runtime vertex and edge.
 */
public final class RuntimeAttributeMap {
  private final Map<RuntimeAttribute.Key, RuntimeAttribute> attributes;
  private final Map<RuntimeAttribute.IntegerKey, Integer> intAttributes;

  public RuntimeAttributeMap() {
    attributes = new HashMap<>();
    intAttributes = new HashMap<>();
  }

  public RuntimeAttribute put(final RuntimeAttribute.Key key, final RuntimeAttribute val) {
    if (!val.hasKey(key)) {
      throw new RuntimeException("Attribute " + val + " is not a member of Key " + key);
    }
    return attributes.put(key, val);
  }

  public Integer put(final RuntimeAttribute.IntegerKey key, final Integer integer) {
    return intAttributes.put(key, integer);
  }

  public RuntimeAttribute get(final RuntimeAttribute.Key key) {
    return attributes.get(key);
  }

  public Integer get(final RuntimeAttribute.IntegerKey key) {
    return intAttributes.get(key);
  }

  public RuntimeAttribute remove(final RuntimeAttribute.Key key) {
    return attributes.remove(key);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(attributes);
    return sb.toString();
  }
}

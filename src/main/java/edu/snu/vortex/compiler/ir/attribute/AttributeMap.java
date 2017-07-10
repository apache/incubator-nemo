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
package edu.snu.vortex.compiler.ir.attribute;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * AttributeMap Class, which uses HashMap for keeping track of attributes for vertices and edges.
 */
public final class AttributeMap implements Serializable {
  private final String id;
  private final Map<Attribute.Key, Attribute> attributes;
  private final Map<Attribute.IntegerKey, Integer> intAttributes;

  /**
   * Constructor for AttributeMap class.
   * @param id ID of the vertex / edge to keep the attribute of.
   */
  private AttributeMap(final String id) {
    this.id = id;
    attributes = new HashMap<>();
    intAttributes = new HashMap<>();
  }

  /**
   * Static initializer for irEdges.
   * @param irEdge irEdge to keep the attributes of.
   * @return The corresponding AttributeMap.
   */
  public static AttributeMap of(final IREdge irEdge) {
    final AttributeMap map = new AttributeMap(irEdge.getId());
    map.setDefaultEdgeValues();
    return map;
  }
  /**
   * Static initializer for irVertex.
   * @param irVertex irVertex to keep the attributes of.
   * @return The corresponding AttributeMap.
   */
  public static AttributeMap of(final IRVertex irVertex) {
    final AttributeMap map = new AttributeMap(irVertex.getId());
    map.setDefaultVertexValues();
    return map;
  }

  /**
   * Putting default attributes for edges.
   */
  private void setDefaultEdgeValues() {
    this.attributes.put(Attribute.Key.Partitioning, Attribute.Hash);
    // TODO #319: Local should be changed to File, upon fixing the bug that prevents integration tests from passing.
    this.attributes.put(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    this.attributes.put(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
  }
  /**
   * Putting default attributes for vertices.
   */
  private void setDefaultVertexValues() {
    this.attributes.put(Attribute.Key.Placement, Attribute.None);
    this.intAttributes.put(Attribute.IntegerKey.Parallelism, 1);
  }

  /**
   * ID of the item this AttributeMap class is keeping track of.
   * @return the ID of the item this AttributeMap class is keeping track of.
   */
  public String getId() {
    return id;
  }

  /**
   * Put a key and a value in this AttributeMap.
   * @param key the key of the attribute.
   * @param val the value of the attribute.
   * @return the attribute.
   */
  public Attribute put(final Attribute.Key key, final Attribute val) {
    if (!val.hasKey(key)) {
      throw new RuntimeException("Attribute " + val + " is not a member of Key " + key);
    }
    return attributes.put(key, val);
  }
  /**
   * Put a key and a value in this AttributeMap.
   * @param key the key of the attribute.
   * @param integer the value of integer of the attribute.
   * @return the attribute.
   */
  public Integer put(final Attribute.IntegerKey key, final Integer integer) {
    return intAttributes.put(key, integer);
  }

  /**
   * Get the value of the given key in this AttributeMap.
   * @param key the key to look for.
   * @return the attribute corresponding to the key.
   */
  public Attribute get(final Attribute.Key key) {
    return attributes.get(key);
  }
  /**
   * Get the value of the given key in this AttributeMap.
   * @param key the key to look for.
   * @return the attribute corresponding to the key.
   */
  public Integer get(final Attribute.IntegerKey key) {
    return intAttributes.get(key);
  }

  /**
   * remove the attribute.
   * @param key key of the attribute to remove.
   * @return the removed attribute.
   */
  public Attribute remove(final Attribute.Key key) {
    return attributes.remove(key);
  }
  /**
   * remove the attribute.
   * @param key key of the attribute to remove.
   * @return the removed attribute.
   */
  public Integer remove(final Attribute.IntegerKey key) {
    return intAttributes.remove(key);
  }

  /**
   * @param key key to look for
   * @return whether or not the attribute map contains the key.
   */
  public boolean containsKey(final Attribute.Key key) {
    return attributes.containsKey(key);
  }
  /**
   * @param key key to look for
   * @return whether or not the attribute map contains the key.
   */
  public boolean containsKey(final Attribute.IntegerKey key) {
    return attributes.containsKey(key);
  }

  /**
   * Same as forEach function in Java 8, but for attributes.
   * @param action action to apply to each attributes.
   */
  public void forEachAttr(final BiConsumer<? super Attribute.Key, ? super Attribute> action) {
    attributes.forEach(action);
  }
  /**
   * Same as forEach function in Java 8, but for attributes.
   * @param action action to apply to each attributes.
   */
  public void forEachIntAttr(final BiConsumer<? super Attribute.IntegerKey, ? super Integer> action) {
    intAttributes.forEach(action);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean isFirstPair = true;
    for (final Map.Entry<Attribute.Key, Attribute> pair : attributes.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(pair.getKey());
      sb.append("\": \"");
      sb.append(pair.getValue());
      sb.append("\"");
    }
    for (final Map.Entry<Attribute.IntegerKey, Integer> pair : intAttributes.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(pair.getKey());
      sb.append("\": ");
      sb.append(pair.getValue());
    }
    sb.append("}");
    return sb.toString();
  }

  // Apache commons-lang 3 Equals/HashCodeBuilder template.
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    AttributeMap that = (AttributeMap) obj;

    return new EqualsBuilder()
        .append(attributes, that.attributes)
        .append(intAttributes, that.intAttributes)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(attributes)
        .append(intAttributes)
        .toHashCode();
  }
}

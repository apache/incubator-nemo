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
package edu.snu.vortex.utils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements {@link DAG}.
 * @param <V> type of the vertex
 */
@NotThreadSafe
public final class DAGImpl<V> implements DAG<V> {
  /**
   * Logger.
   */
  private static final Logger LOG = Logger.getLogger(DAGImpl.class.getName());
  /**
   * A set of root vertices.
   */
  private final Set<V> rootVertices = new HashSet<>();

  /**
   * A map containing <vertex, Set of its parent vertices> mappings.
   */
  private final Map<V, Set<V>> parentVertices = new HashMap<>();

  /**
   * A map containing <vertex, Set of its children vertices> mappings.
   */
  private final Map<V, Set<V>> childrenVertices = new HashMap<>();

  @Override
  public Set<V> getRootVertices() {
    return Collections.unmodifiableSet(rootVertices);
  }

  @Override
  public boolean addVertex(final V v) {
    if (!childrenVertices.containsKey(v) && !parentVertices.containsKey(v)) {
      childrenVertices.put(v, new HashSet<>());
      parentVertices.put(v, new HashSet<>());
      rootVertices.add(v);
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} already exists", v);
      return false;
    }
  }

  @Override
  public boolean removeVertex(final V v) {
    final Set<V> children = childrenVertices.remove(v);
    if (children != null) {
      children.forEach(child -> {
        final Set<V> parents = parentVertices.get(child);
        parents.remove(v);
        if (parents.isEmpty()) {
          rootVertices.add(child);
        }
      });
      rootVertices.remove(v);
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} does exists", v);
      return false;
    }
  }

  @Override
  public boolean addEdge(final V src, final V dst) {
    if (!childrenVertices.containsKey(src) || !parentVertices.containsKey(src)) {
      throw new NoSuchElementException("No src vertex " + src);
    }
    if (!childrenVertices.containsKey(dst) || !parentVertices.containsKey(dst)) {
      throw new NoSuchElementException("No dest vertex " + dst);
    }

    if (isADescendant(dst, src)) {
      throw new IllegalStateException("The edge from " + src + " to " + dst
          + " makes a cycle in the graph");
    }

    final Set<V> childrenOfSrc = childrenVertices.get(src);
    if (childrenOfSrc.add(dst)) {
      parentVertices.get(dst).add(src);
      rootVertices.remove(dst);
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} already exists", new Object[]{src, dst});
      return false;
    }
  }

  @Override
  public boolean removeEdge(final V src, final V dst) {
    if (!childrenVertices.containsKey(src) || !parentVertices.containsKey(src)) {
      throw new NoSuchElementException("No src vertex " + src);
    }
    if (!childrenVertices.containsKey(dst) || !parentVertices.containsKey(dst)) {
      throw new NoSuchElementException("No dest vertex " + dst);
    }

    final Set<V> childrenOfSrc = childrenVertices.get(src);
    if (childrenOfSrc.remove(dst)) {
      final Set<V> parentsOfDst = parentVertices.get(dst);
      parentsOfDst.remove(src);
      if (parentsOfDst.size() == 1) {
        rootVertices.add(dst);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} does not exists", new Object[]{src, dst});
      return false;
    }
  }

  /**
   * Checks whether a vertex is a descendant of the other.
   * @param v the potential ancestor
   * @param w the potential descendant
   * @return true if it is a descendant, false otherwise
   */
  private boolean isADescendant(final V v, final V w) {
    if (v.equals(w)) {
      return true;
    }

    final Set<V> children = childrenVertices.get(v);
    for (final V child : children) {
      if (isADescendant(child, w)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("DAGImpl{");
    sb.append("rootVertices=").append(rootVertices);
    sb.append(", parentVertices=").append(parentVertices);
    sb.append(", childrenVertices=").append(childrenVertices);
    sb.append('}');
    return sb.toString();
  }
}

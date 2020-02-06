/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nemo.common;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A KeyExtractor for Pair class.
 */
public final class PairKeyExtractor implements KeyExtractor {
  @Override
  public Object extractKey(final Object element) {
    if (element instanceof Pair) {
      return ((Pair) element).left();
    } else {
      throw new IllegalStateException(element.toString());
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    return o != null && getClass() == o.getClass();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(133, 37).toHashCode();
  }
}

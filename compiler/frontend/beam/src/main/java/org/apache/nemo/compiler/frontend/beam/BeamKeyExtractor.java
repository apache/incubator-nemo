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
package org.apache.nemo.compiler.frontend.beam;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nemo.common.KeyExtractor;
import org.apache.beam.sdk.values.KV;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
final class BeamKeyExtractor implements KeyExtractor {
  @Override
  public Object extractKey(final Object element) {
    final Object valueToExtract = element instanceof WindowedValue ? ((WindowedValue) element).getValue() : element;
    if (valueToExtract instanceof KV) {
      // Handle null keys, since Beam allows KV with null keys.
      final Object key = ((KV) valueToExtract).getKey();
      return key == null ? 0 : key;
    } else {
      return element;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).toHashCode();
  }
}

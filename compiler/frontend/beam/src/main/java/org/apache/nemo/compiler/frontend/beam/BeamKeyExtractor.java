/*
 * Copyright (C) 2018 Seoul National University
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
package org.apache.nemo.compiler.frontend.beam;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.nemo.common.KeyExtractor;
import org.apache.beam.sdk.values.KV;

import java.util.Arrays;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
final class BeamKeyExtractor implements KeyExtractor {
  @Override
  public Object extractKey(final Object element) {
    final WindowedValue windowedValue = (WindowedValue) element;
    final Object value = windowedValue.getValue();
    if (value instanceof KV) {
      // Handle null keys, since Beam allows KV with null keys.
      final Object key = ((KV) value).getKey();
      if (key == null) {
        return 0;
      } else if (key instanceof Row) {
        // TODO #223: Use Row.hashCode in BeamKeyExtractor
        return Arrays.hashCode(((Row) key).getValues().toArray());
      } else {
        return key;
      }
    } else {
      return element;
    }
  }
}

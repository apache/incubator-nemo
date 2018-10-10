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
package org.apache.nemo.compiler.optimizer;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;

/**
 * Extracts the key from a pair element.
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
}

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
package edu.snu.nemo.common;

import java.io.Serializable;

/**
 * Extracts a key from an element.
 * Keys are used for partitioning.
 */
public interface KeyExtractor extends Serializable {
  /**
   * Extracts key.
   * @param element Element to get the key from.
   * @return The extracted key of the element.
   */
  Object extractKey(final Object element);
}

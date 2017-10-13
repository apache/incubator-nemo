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
package edu.snu.onyx.compiler.ir;

import java.io.Serializable;

/**
 * Key-value pair wrapper for a data element.
 * This is to be implemented in the frontend
 * with language-specific key-value pair definition.
 * @param <Data> data type.
 * @param <Key> key type.
 * @param <Value> value type.
 */
public interface Element<Data, Key, Value> extends Serializable {
  /**
   * @return data.
   */
  Data getData();

  /**
   * @return key.
   */
  Key getKey();

  /**
   * @return value.
   */
  Value getValue();
}

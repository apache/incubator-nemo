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
package edu.snu.nemo.runtime.executor.data.streamchainer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link EncodeStreamChainer} object indicates each stream manipulation strategy.
 * Stream can be chained by {@link EncodeStreamChainer} multiple times.
 */
public interface EncodeStreamChainer {

  /**
   * Chain {@link OutputStream} and returns chained {@link OutputStream}.
   *
   * @param out the stream which will be chained.
   * @return chained {@link OutputStream}.
   * @throws IOException if fail to chain the stream.
   */
  OutputStream chainOutput(OutputStream out) throws IOException;
}

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
import java.util.List;
import java.util.Map;

/**
 * Interface for specifying 'What' to do with data.
 * It is to be implemented in the compiler frontend, possibly for every operator in a dataflow language.
 * 'How' and 'When' to do with its input/output data are up to the runtime.
 */
public interface Transform extends Serializable {

  /**
   * Prepare the transform.
   * @param context of the transform.
   * @param outputCollector that collects outputs.
   */
  void prepare(Context context, OutputCollector outputCollector);

  /**
   * On data received.
   * @param data data received.
   * @param srcVertexId sender of the data.
   */
  void onData(Iterable<Element> data, String srcVertexId);

  /**
   * Close the transform.
   */
  void close();

  /**
   * Context of the transform.
   * It is currently unused, but might come in handy
   * when we have more sophisticated operations like Join.
   */
  interface Context {
    /**
     * @return source vertex ids.
     */
    List<String> getSrcVertexIds();

    /**
     * @return destination vertex ids.
     */
    List<String> getDstVertexIds();

    /**
     * @return sideInputs.
     */
    Map<Transform, Object> getSideInputs();
  }
}

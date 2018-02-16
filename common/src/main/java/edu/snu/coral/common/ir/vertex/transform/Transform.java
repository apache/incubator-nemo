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
package edu.snu.coral.common.ir.vertex.transform;

import edu.snu.coral.common.ir.Pipe;
import java.io.Serializable;
import java.util.Map;

/**
 * Interface for specifying 'What' to do with data.
 * It is to be implemented in the compiler frontend, possibly for every operator in a dataflow language.
 * 'How' and 'When' to do with its input/output data are up to the runtime.
 * @param <I> input type.
 * @param <O> output type.
 */
public interface Transform<I, O> extends Serializable {
  /**
   * Prepare the transform.
   * @param context of the transform.
   * @param pipe that collects outputs.
   */
  void prepare(Context context, Pipe<O> pipe);

  /**
   * On data received.
   * @param element data received.
   */
  void onData(Object element);

  /**
   * Close the transform.
   */
  void close();

  /**
   * Context of the transform.
   */
  interface Context {
    /**
     * @return sideInputs.
     */
    Map<Transform, Object> getSideInputs();
  }
}

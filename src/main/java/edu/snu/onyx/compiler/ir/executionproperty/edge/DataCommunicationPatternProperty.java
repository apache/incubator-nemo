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
package edu.snu.onyx.compiler.ir.executionproperty.edge;

import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.executor.datatransfer.communication.DataCommunicationPattern;

/**
 * DataCommunicationPattern ExecutionProperty.
 */
public final class DataCommunicationPatternProperty
    extends ExecutionProperty<Class<? extends DataCommunicationPattern>> {
  private DataCommunicationPatternProperty(final Class<? extends DataCommunicationPattern> value) {
    super(Key.DataCommunicationPattern, value);
  }

  public static DataCommunicationPatternProperty of(final Class<? extends DataCommunicationPattern> value) {
    return new DataCommunicationPatternProperty(value);
  }
}

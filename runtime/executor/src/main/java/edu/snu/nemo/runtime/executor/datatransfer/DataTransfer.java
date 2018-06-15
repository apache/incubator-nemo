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
package edu.snu.nemo.runtime.executor.datatransfer;


/**
 * Contains common parts involved in {@link InputReader} and {@link OutputWriter}.
 * The two classes are involved in
 * intermediate data transfer between {@link edu.snu.nemo.runtime.common.plan.physical.Task}.
 */
public abstract class DataTransfer {
  private final String id;

  public DataTransfer(final String id) {
    this.id = id;
  }

  /**
   * @return ID of the reader/writer.
   */
  public final String getId() {
    return id;
  }
}

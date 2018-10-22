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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.PipeManagerWorker;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the input data transfer to a task.
 */
public final class PipeInputReader extends InputReader {
  final PipeManagerWorker pipeManagerWorker;

  public PipeInputReader(final PipeManagerWorker pipeManagerWorker) {
    super();
    this.pipeManagerWorker = pipeManagerWorker;
  }

  @Override
  CompletableFuture<DataUtil.IteratorWithNumBytes> readOneToOne() {
    // read one pipe
    pipeManagerWorker.read();
  }

  @Override
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast() {
    // read many broadcast pipes
    pipeManagerWorker.read();
  }

  @Override
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange() {
    // read many shuffle pipes
    pipeManagerWorker.read();
  }
}

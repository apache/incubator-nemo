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
package edu.snu.nemo.compiler.frontend.spark.core;

import edu.snu.nemo.runtime.executor.data.BroadcastManagerWorker;
import scala.reflect.ClassTag$;

/**
 * @param <T> type of the broadcast data.
 */
public final class SparkBroadcast<T> extends org.apache.spark.broadcast.Broadcast<T> {
  private final long tag;

  SparkBroadcast(final long tag, final Class<T> classType) {
    super(tag, ClassTag$.MODULE$.apply(classType));
    this.tag = tag;
  }

  @Override
  public T getValue() {
    return (T) BroadcastManagerWorker.staticReference.get(tag);
  }

  @Override
  public void doUnpersist(final boolean blocking) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doDestroy(final boolean blocking) {
    throw new UnsupportedOperationException();
  }
}

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

public final class SparkBroadcast<T> extends org.apache.spark.broadcast.Broadcast<T> {
  final Object tag;

  public SparkBroadcast(final Object tag) {
    this.tag = ta;
  }

  @Override
  public T getValue() {
    // TODO: Transform.Context.getSideInputs();
    // TODO: use the tag (needs wireups...)
    // Problem: this object resides inside the IRVertex
    return null;
  }

  @Override
  public void doUnpersist(boolean blocking) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doDestroy(boolean blocking) {
    throw new UnsupportedOperationException();
  }
}

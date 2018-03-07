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

import edu.snu.nemo.common.coder.Coder;

import java.util.List;

/**
 * class that contains {@link Coder} and {@link List} of {@link StreamChainer}.
 * @param <T> coder element type.
 */
public final class Serializer<T> {
  private Coder<T> coder;
  private List<StreamChainer> streamChainers;

  /**
   * Constructor.
   *
   * @param coder      {@link Coder}.
   * @param streamChainers list of {@link StreamChainer}.
   */
  public Serializer(final Coder<T> coder, final List<StreamChainer> streamChainers) {
    this.coder = coder;
    this.streamChainers = streamChainers;
  }

  /**
   * method that returns {@link Coder}.
   *
   * @return {@link Coder}.
   */
  public Coder<T> getCoder() {
    return coder;
  }

  /**
   * method that returns list of {@link StreamChainer}.
   *
   * @return list of {@link StreamChainer}.
   */
  public List<StreamChainer> getStreamChainers() {
    return streamChainers;
  }

  /**
   * method that sets list of {@link StreamChainer}.
   *
   * @param streamChainers list of {@link StreamChainer}.
   */
  public void setStreamChainers(final List<StreamChainer> streamChainers) {
    this.streamChainers = streamChainers;
  }
}

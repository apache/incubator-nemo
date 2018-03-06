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
package edu.snu.nemo.runtime.executor.data;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class provide functionality to limit bytes read from {@link InputStream}.
 * You need to wrap chained compression stream with this stream to prevent overreading
 * inner stream.
 */
public final class LimitedInputStream extends InputStream {
  private final InputStream in;
  private long limit;

  /**
   * Constructor.
   *
   * @param in    {@link InputStream} that should be limited.
   * @param limit bytes to limit.
   */
  public LimitedInputStream(final InputStream in, final long limit) {
    this.in = in;
    this.limit = limit;
  }

  @Override
  public int read() throws IOException {
    if (limit > 0) {
      limit--;
      return in.read();
    } else {
      return -1;
    }
  }
}

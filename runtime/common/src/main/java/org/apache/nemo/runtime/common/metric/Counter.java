/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.common.metric;

import java.io.Serializable;

/**
 * Counter class for some metrics
 */
public class Counter implements Serializable {
  private long val = 0;
  public void reset() {
    val = 0;
  }

  public void reset(long n) {
    val = n;
  }

  public void inc() {
    val += 1;
  }

  public void inc(long n) {
    val += n;
  }

  public void dec() {
    val -= 1;
  }

  public void dec(long n) {
    val -= n;
  }

  public long getVal() {
    return val;
  }
}

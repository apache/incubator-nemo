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
package org.apache.nemo.compiler.frontend.beam;

/**
 * {@link org.apache.nemo.compiler.frontend.beam.transform.DoFnTransform} treats elements of this type as side inputs.
 * TODO #289: Prevent using SideInputElement in UDFs
 * @param <T> type of the side input value.
 */
public final class SideInputElement<T> {
  private final int sideInputIndex;
  private final T sideInputValue;

  public SideInputElement(final int sideInputIndex, final T sideInputValue) {
    this.sideInputIndex = sideInputIndex;
    this.sideInputValue = sideInputValue;
  }

  public int getSideInputIndex() {
    return sideInputIndex;
  }

  public T getSideInputValue() {
    return sideInputValue;
  }

  @Override
  public String toString() {
    return "SideInput: (" + sideInputIndex + ", " + sideInputValue.toString() + ")";
  }
}

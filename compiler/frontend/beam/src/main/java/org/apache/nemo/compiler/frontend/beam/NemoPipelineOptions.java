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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * NemoPipelineOptions.
 */
public interface NemoPipelineOptions extends PipelineOptions {
  /**
   * @return the maximum number of elements in a bundle.
   */
  @Description("The maximum number of elements in a bundle.")
  @Default.Long(1000)
  Long getMaxBundleSize();

  /**
   * @param size the maximum number of elements in a bundle.
   */
  void setMaxBundleSize(Long size);

  /**
   * @return the maximum time to wait before finalising a bundle (in milliseconds).
   */
  @Description("The maximum time to wait before finalising a bundle (in milliseconds).")
  @Default.Long(1000)
  Long getMaxBundleTimeMills();

  /**
   * @param time the maximum time to wait before finalising a bundle (in milliseconds).
   */
  void setMaxBundleTimeMills(Long time);
}

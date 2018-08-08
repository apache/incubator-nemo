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
package edu.snu.nemo.common;

import edu.snu.nemo.common.ir.vertex.transform.Transform;

import java.util.Map;
import java.util.Optional;

/**
 * Transform Context Implementation.
 */
public final class ContextImpl implements Transform.Context {
  private final Map sideInputs;
  private final Optional<String> mainTag;
  private final Map<String, String> tagToAdditionalChildren;
  private String data;

  /**
   * Constructor of Context Implementation.
   * @param sideInputs              side inputs.
   * @param tagToAdditionalChildren tag id to additional vertices id map.
   */
  public ContextImpl(final Map sideInputs,
                     final Optional<String> mainTag,
                     final Map<String, String> tagToAdditionalChildren) {
    this.sideInputs = sideInputs;
    this.mainTag =  mainTag;
    this.tagToAdditionalChildren = tagToAdditionalChildren;
    this.data = null;
  }

  @Override
  public Map getSideInputs() {
    return this.sideInputs;
  }

  @Override
  public Map<String, String> getTagToAdditionalChildren() {
    return this.tagToAdditionalChildren;
  }

  @Override
  public Optional<String> getMainTag() {
    return mainTag;
  }

  @Override
  public void setSerializedData(final String serializedData) {
    this.data = serializedData;
  }

  @Override
  public Optional<String> getSerializedData() {
    return Optional.ofNullable(this.data);
  }
}

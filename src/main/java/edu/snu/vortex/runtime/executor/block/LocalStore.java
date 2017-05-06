/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.compiler.ir.Element;

import java.util.HashMap;
import java.util.Optional;

/**
 * Store data in local memory, unserialized.
 */
public final class LocalStore implements BlockStore {
  private final HashMap<String, Iterable<Element>> blockIdToData;

  public LocalStore() {
    this.blockIdToData = new HashMap<>();
  }

  public Optional<Iterable<Element>> getBlock(final String blockId) {
    return Optional.ofNullable(blockIdToData.get(blockId));
  }

  public void putBlock(final String blockId, final Iterable<Element> data) {
    if (blockIdToData.containsKey(blockId)) {
      throw new RuntimeException("Trying to overwrite an already-put block");
    }
    blockIdToData.put(blockId, data);
  }
}

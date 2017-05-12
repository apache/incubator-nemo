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

import java.util.Optional;

/**
 * Interface for block placement.
 */
public interface BlockStore {
  /**
   * Retrieves a block.
   * @param blockId of the block
   * @return the data of the block (optionally)
   */
  Optional<Iterable<Element>> getBlock(String blockId);

  /**
   * Saves a block.
   * @param blockId of the block
   * @param data of the block
   */
  void putBlock(String blockId, Iterable<Element> data);
}

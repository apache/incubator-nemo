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
package edu.snu.nemo.compiler.frontend.beam;

import edu.snu.nemo.common.KeyExtractor;
import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.common.coder.EncoderFactory;
import org.apache.beam.sdk.values.KV;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
final class BeamKeyExtractor implements KeyExtractor {
  private EncoderFactory keyEncoderFactory;
  private DecoderFactory keyDecoderFactory;

  BeamKeyExtractor(final EncoderFactory keyEncoderFactory,
                   final DecoderFactory keyDecoderFactory) {
    this.keyEncoderFactory = keyEncoderFactory;
    this.keyDecoderFactory = keyDecoderFactory;
  }

  @Override
  public Object extractKey(final Object element) {
    if (element instanceof KV) {
      // Handle null keys, since Beam allows KV with null keys.
      final Object key = ((KV) element).getKey();
      return key == null ? 0 : key;
    } else {
      return element;
    }
  }

  @Override
  public EncoderFactory getKeyEncoderFactory() {
    if (keyEncoderFactory == null) {
      throw new RuntimeException("Extracting keyEncoderFactory from non-shuffle edge");
    }
    return keyEncoderFactory;
  }

  @Override
  public DecoderFactory getKeyDecoderFactory() {
    if (keyEncoderFactory == null) {
      throw new RuntimeException("Extracting keyDecoderFactory from non-shuffle edge");
    }
    return keyDecoderFactory;
  }
}

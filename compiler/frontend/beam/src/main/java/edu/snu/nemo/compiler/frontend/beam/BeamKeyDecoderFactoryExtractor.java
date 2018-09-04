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

import edu.snu.nemo.common.KeyDecoderFactoryExtractor;
import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
final class BeamKeyDecoderFactoryExtractor implements KeyDecoderFactoryExtractor {
  @Override
  public DecoderFactory extractKeyDecoderFactory(final DecoderFactory decoderFactory) {
    Coder coder = (Coder) decoderFactory.getCoder();
    if (coder instanceof KvCoder) {
      final Coder beamKeyCoder = ((KvCoder) coder).getKeyCoder();
      return new BeamDecoderFactory(beamKeyCoder);
    } else {
      throw new RuntimeException("Cannot extract key decoder factory - coder is not of type KvCoder");
    }
  }
}

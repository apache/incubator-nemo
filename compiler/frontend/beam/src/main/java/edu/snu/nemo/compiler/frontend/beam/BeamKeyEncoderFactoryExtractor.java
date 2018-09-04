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

import edu.snu.nemo.common.KeyEncoderFactoryExtractor;
import edu.snu.nemo.common.coder.EncoderFactory;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;

/**
 * Extracts the key from a KV element.
 * For non-KV elements, the elements themselves become the key.
 */
final class BeamKeyEncoderFactoryExtractor implements KeyEncoderFactoryExtractor {
  @Override
  public EncoderFactory extractKeyEncoderFactory(final EncoderFactory encoderFactory) {
    Coder coder = (Coder) encoderFactory.getCoder();
    if (coder instanceof KvCoder) {
      final Coder beamKeyCoder = ((KvCoder) coder).getKeyCoder();
      return new BeamEncoderFactory(beamKeyCoder);
    } else {
      throw new RuntimeException("Cannot extract key encoder factory - coder is not of type KvCoder");
    }
  }
}

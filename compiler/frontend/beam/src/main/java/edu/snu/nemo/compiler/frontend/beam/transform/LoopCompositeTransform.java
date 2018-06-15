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
package edu.snu.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import java.io.Serializable;

/**
 * A composite transform for wrapping transforms inside a loop to create loop operators in the IR.
 * Each iterations are stateless, as the repeating iterations are zipped into a single copy.
 * We assume a single {@link LoopCompositeTransform} inside a for/while loop.
 * @param <inputT> input type of the composite transform.
 * @param <outputT> output type of the composite transform.
 */
public abstract class LoopCompositeTransform<inputT extends PInput, outputT extends POutput>
    extends PTransform<inputT, outputT> implements Serializable {
}

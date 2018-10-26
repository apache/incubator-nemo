package org.apache.nemo.compiler.frontend.beam.transform;

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

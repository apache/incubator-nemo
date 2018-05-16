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
package edu.snu.nemo.examples.beam;

import edu.snu.nemo.compiler.frontend.beam.coder.FloatArrayCoder;
import edu.snu.nemo.compiler.frontend.beam.coder.IntArrayCoder;
import edu.snu.nemo.compiler.frontend.beam.transform.LoopCompositeTransform;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Alternating Least Square application.
 * This application have been made separately, to demonstrate the LoopInvariantCodeMotion optimization pass.
 * This takes the unnecessarily repetitive code that parses user data in every loop, to be performed just a single time.
 */
public final class AlternatingLeastSquareInefficient {
  private static final Logger LOG = LoggerFactory.getLogger(AlternatingLeastSquare.class.getName());

  /**
   * Private constructor.
   */
  private AlternatingLeastSquareInefficient() {
  }

  /**
   * Composite transform that wraps the transforms inside the loop.
   * The loop updates the user matrix and the item matrix in each iteration.
   */
  public static final class UpdateUserAndItemMatrix extends LoopCompositeTransform<
      PCollection<KV<Integer, float[]>>, PCollection<KV<Integer, float[]>>> {
    private final Integer numFeatures;
    private final Double lambda;
    private final PCollection<String> rawData;
    private final PCollection<KV<Integer, KV<int[], float[]>>> parsedItemData;

    /**
     * Constructor of UpdateUserAndItemMatrix CompositeTransform.
     * @param numFeatures number of features.
     * @param lambda lambda.
     * @param rawData PCollection of raw data.
     * @param parsedItemData PCollection of parsed item data.
     */
    UpdateUserAndItemMatrix(final Integer numFeatures, final Double lambda,
                            final PCollection<String> rawData,
                            final PCollection<KV<Integer, KV<int[], float[]>>> parsedItemData) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.rawData = rawData;
      this.parsedItemData = parsedItemData;
    }

    @Override
    public PCollection<KV<Integer, float[]>> expand(final PCollection<KV<Integer, float[]>> itemMatrix) {
      // Parse data for user
      final PCollection<KV<Integer, KV<int[], float[]>>> parsedUserData = rawData
          .apply(ParDo.of(new AlternatingLeastSquare.ParseLine(true)))
          .apply(Combine.perKey(new AlternatingLeastSquare.TrainingDataCombiner()));

      // Make Item Matrix view.
      final PCollectionView<Map<Integer, float[]>> itemMatrixView = itemMatrix.apply(View.asMap());
      // Get new User Matrix
      final PCollectionView<Map<Integer, float[]>> userMatrixView = parsedUserData
          .apply(ParDo.of(new AlternatingLeastSquare.CalculateNextMatrix(numFeatures, lambda, itemMatrixView))
              .withSideInputs(itemMatrixView))
          .apply(View.asMap());
      // return new Item Matrix
      return parsedItemData.apply(
          ParDo.of(new AlternatingLeastSquare.CalculateNextMatrix(numFeatures, lambda, userMatrixView))
          .withSideInputs(userMatrixView));
    }
  }

  /**
   * Main function for the ALS BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final Long start = System.currentTimeMillis();
    LOG.info(Arrays.toString(args));
    final String inputFilePath = args[0];
    final Integer numFeatures = Integer.parseInt(args[1]);
    final Integer numItr = Integer.parseInt(args[2]);
    final Double lambda;
    if (args.length > 4) {
      lambda = Double.parseDouble(args[3]);
    } else {
      lambda = 0.05;
    }

    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("ALS");
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);

    final Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(int[].class, IntArrayCoder.class));
    p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(float[].class, FloatArrayCoder.class));

    // Read raw data
    final PCollection<String> rawData = GenericSourceSink.read(p, inputFilePath);

    // Parse data for item
    final PCollection<KV<Integer, KV<int[], float[]>>> parsedItemData = rawData
        .apply(ParDo.of(new AlternatingLeastSquare.ParseLine(false)))
        .apply(Combine.perKey(new AlternatingLeastSquare.TrainingDataCombiner()));

    // Create Initial Item Matrix
    PCollection<KV<Integer, float[]>> itemMatrix = parsedItemData
        .apply(ParDo.of(new DoFn<KV<Integer, KV<int[], float[]>>, KV<Integer, float[]>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) throws Exception {
            final float[] result = new float[numFeatures];

            final KV<Integer, KV<int[], float[]>> element = c.element();
            final float[] ratings = element.getValue().getValue();
            for (int i = 0; i < ratings.length; i++) {
              result[0] += ratings[i];
            }

            result[0] /= ratings.length;
            for (int i = 1; i < result.length; i++) {
              result[i] = (float) (Math.random() * 0.01);
            }
            c.output(KV.of(element.getKey(), result));
          }
        }));

    // Iterations to update Item Matrix.
    for (Integer i = 0; i < numItr; i++) {
      // NOTE: a single composite transform for the iteration.
      itemMatrix = itemMatrix.apply(new UpdateUserAndItemMatrix(numFeatures, lambda, rawData, parsedItemData));
    }

    p.run();
    LOG.info("JCT " + (System.currentTimeMillis() - start));
  }
}

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
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.client.beam.LoopCompositeTransform;
import edu.snu.vortex.common.Pair;
import edu.snu.vortex.compiler.frontend.beam.Runner;
import edu.snu.vortex.compiler.frontend.beam.coder.PairCoder;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sample Alternating Least Square application.
 * This application have been made separately, to demonstrate the LoopInvariantCodeMotion optimization pass.
 * This takes the unnecessarily repetitive code that parses user data in every loop, to be performed just a single time.
 */
public final class AlternatingLeastSquareInefficient {
  private static final Logger LOG = Logger.getLogger(AlternatingLeastSquare.class.getName());

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
      PCollection<KV<Integer, List<Double>>>, PCollection<KV<Integer, List<Double>>>> {
    private final Integer numFeatures;
    private final Double lambda;
    private final PCollection<String> rawData;
    private final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedItemData;

    /**
     * Constructor of UpdateUserAndItemMatrix CompositeTransform.
     * @param numFeatures number of features.
     * @param lambda lambda.
     * @param rawData PCollection of raw data.
     * @param parsedItemData PCollection of parsed item data.
     */
    UpdateUserAndItemMatrix(final Integer numFeatures, final Double lambda,
                            final PCollection<String> rawData,
                            final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedItemData) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.rawData = rawData;
      this.parsedItemData = parsedItemData;
    }

    @Override
    public PCollection<KV<Integer, List<Double>>> expand(final PCollection<KV<Integer, List<Double>>> itemMatrix) {
      // Parse data for user
      final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedUserData = rawData
          .apply(ParDo.of(new AlternatingLeastSquare.ParseLine(true)))
          .apply(Combine.perKey(new AlternatingLeastSquare.TrainingDataCombiner()));

      // Make Item Matrix view.
      final PCollectionView<Map<Integer, List<Double>>> itemMatrixView = itemMatrix.apply(View.asMap());
      // Get new User Matrix
      final PCollectionView<Map<Integer, List<Double>>> userMatrixView = parsedUserData
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
    LOG.log(Level.INFO, Arrays.toString(args));
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
    options.setRunner(Runner.class);
    options.setJobName("ALS");
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);

    final Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(Pair.class, PairCoder.class));

    // Read raw data
    final PCollection<String> rawData = GenericSourceSink.read(p, inputFilePath);

    // Parse data for item
    final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedItemData = rawData
        .apply(ParDo.of(new AlternatingLeastSquare.ParseLine(false)))
        .apply(Combine.perKey(new AlternatingLeastSquare.TrainingDataCombiner()));

    // Create Initial Item Matrix
    PCollection<KV<Integer, List<Double>>> itemMatrix = parsedItemData
        .apply(ParDo.of(new DoFn<KV<Integer, Pair<List<Integer>, List<Double>>>, KV<Integer, List<Double>>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) throws Exception {
            final List<Double> result = new ArrayList<>(numFeatures);
            result.add(0, 0.0);

            final KV<Integer, Pair<List<Integer>, List<Double>>> element = c.element();
            final List<Double> ratings = element.getValue().right();
            for (Integer i = 0; i < ratings.size(); i++) {
              result.set(0, result.get(0) + ratings.get(i));
            }

            result.set(0, result.get(0) / ratings.size());
            for (Integer i = 1; i < result.size(); i++) {
              result.add(i, (Math.random() * 0.01));
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
    LOG.log(Level.INFO, "JCT " + (System.currentTimeMillis() - start));
  }
}

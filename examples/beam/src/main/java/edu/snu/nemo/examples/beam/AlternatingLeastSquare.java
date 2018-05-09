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

import edu.snu.nemo.compiler.frontend.beam.transform.LoopCompositeTransform;
import edu.snu.nemo.compiler.frontend.beam.coder.PairCoder;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang.ArrayUtils;
import org.netlib.util.intW;
import com.github.fommil.netlib.BLAS;
import com.github.fommil.netlib.LAPACK;

import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Alternating Least Square application.
 */
public final class AlternatingLeastSquare {
  private static final Logger LOG = LoggerFactory.getLogger(AlternatingLeastSquare.class.getName());

  /**
   * Private constructor.
   */
  private AlternatingLeastSquare() {
  }

  /**
   * Method for parsing the input line.
   */
  public static final class ParseLine extends DoFn<String, KV<Integer, Pair<List<Integer>, List<Double>>>> {
    private final Boolean isUserData;

    /**
     * Constructor for Parseline DoFn class.
     * @param isUserData flag that distinguishes user data from item data.
     */
    public ParseLine(final Boolean isUserData) {
      this.isUserData = isUserData;
    }

    /**
     * ProcessElement method for BEAM.
     * @param c Process context.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final String text = c.element().trim();
      if (text.startsWith("#") || text.length() == 0) {
        // comments and empty lines
        return;
      }

      final String[] split = text.split("\\s+|:");
      final Integer userId = Integer.parseInt(split[0]);
      final Integer itemId = Integer.parseInt(split[1]);
      final Double rating = Double.parseDouble(split[2]);


      final List<Integer> userList = new ArrayList<>(1);
      userList.add(userId);

      final List<Integer> itemList = new ArrayList<>(1);
      itemList.add(itemId);

      final List<Double> ratingList = new ArrayList<>(1);
      ratingList.add(rating);

      if (isUserData) {
        c.output(KV.of(userId, Pair.of(itemList, ratingList)));
      } else {
        c.output(KV.of(itemId, Pair.of(userList, ratingList)));
      }
    }
  }

  /**
   * A DoFn that relays a single vector list.
   */
  public static final class UngroupSingleVectorList
      extends DoFn<KV<Integer, Iterable<List<Double>>>, KV<Integer, List<Double>>> {
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final KV<Integer, Iterable<List<Double>>> element = c.element();
      final List<List<Double>> listOfVectorLists = new ArrayList<>();
      element.getValue().forEach(listOfVectorLists::add);
      if (listOfVectorLists.size() != 1) {
        throw new RuntimeException("Only a single vector list is expected");
      }

      // Output the ungrouped single vector list
      c.output(KV.of(element.getKey(), listOfVectorLists.get(0)));
    }
  }

  /**
   * Combiner for the training data.
   */
  public static final class TrainingDataCombiner extends Combine.CombineFn<Pair<List<Integer>, List<Double>>,
      List<Pair<List<Integer>, List<Double>>>, Pair<List<Integer>, List<Double>>> {

    @Override
    public List<Pair<List<Integer>, List<Double>>> createAccumulator() {
      return new LinkedList<>();
    }

    @Override
    public List<Pair<List<Integer>, List<Double>>> addInput(final List<Pair<List<Integer>, List<Double>>> accumulator,
                                                            final Pair<List<Integer>, List<Double>> value) {
      accumulator.add(value);
      return accumulator;
    }

    @Override
    public List<Pair<List<Integer>, List<Double>>> mergeAccumulators(
        final Iterable<List<Pair<List<Integer>, List<Double>>>> accumulators) {
      final List<Pair<List<Integer>, List<Double>>> merged = new LinkedList<>();
      accumulators.forEach(merged::addAll);
      return merged;
    }

    @Override
    public Pair<List<Integer>, List<Double>> extractOutput(final List<Pair<List<Integer>, List<Double>>> accumulator) {
      Integer dimension = 0;
      for (final Pair<List<Integer>, List<Double>> pair : accumulator) {
        dimension += pair.left().size();
      }

      final List<Integer> intList = new ArrayList<>(dimension);
      final List<Double> doubleList = new ArrayList<>(dimension);

      Integer itr = 0;
      for (final Pair<List<Integer>, List<Double>> pair : accumulator) {
        final List<Integer> ints = pair.left();
        final List<Double> floats = pair.right();
        for (Integer i = 0; i < ints.size(); i++) {
          intList.add(itr, ints.get(i));
          doubleList.add(itr, floats.get(i));
          itr++;
        }
      }

      return Pair.of(intList, doubleList);
    }
  }

  /**
   * DoFn for calculating next matrix at each iteration.
   */
  public static final class CalculateNextMatrix
      extends DoFn<KV<Integer, Pair<List<Integer>, List<Double>>>, KV<Integer, List<Double>>> {
    private static final LAPACK NETLIB_LAPACK = LAPACK.getInstance();
    private static final BLAS NETLIB_BLAS = BLAS.getInstance();

    private final List<KV<Integer, List<Double>>> results;
    private final Double[] upperTriangularLeftMatrix;
    private final Integer numFeatures;
    private final Double lambda;
    private final PCollectionView<Map<Integer, List<Double>>> fixedMatrixView;

    /**
     * Constructor for CalculateNextMatrix DoFn class.
     *
     * @param numFeatures     number of features.
     * @param lambda          lambda.
     * @param fixedMatrixView a PCollectionView of the fixed matrix (item / user matrix).
     */
    CalculateNextMatrix(final Integer numFeatures, final Double lambda,
                        final PCollectionView<Map<Integer, List<Double>>> fixedMatrixView) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.fixedMatrixView = fixedMatrixView;
      this.results = new LinkedList<>();
      this.upperTriangularLeftMatrix = new Double[numFeatures * (numFeatures + 1) / 2];
    }

    /**
     * ProcessElement method for BEAM.
     *
     * @param c ProcessContext.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      for (Integer j = 0; j < upperTriangularLeftMatrix.length; j++) {
        upperTriangularLeftMatrix[j] = 0.0;
      }

      final Map<Integer, List<Double>> fixedMatrix = c.sideInput(fixedMatrixView);

      final List<Integer> indexArr = c.element().getValue().left();
      final List<Double> ratingArr = c.element().getValue().right();

      final Integer size = indexArr.size();

      final List<Double> vector = new ArrayList<>(numFeatures);
      final double[] rightSideVector = new double[numFeatures];
      final Double[] conf = new Double[numFeatures];
      for (Integer i = 0; i < size; i++) {
        final Integer ratingIndex = indexArr.get(i);
        final Double rating = ratingArr.get(i);
        for (Integer j = 0; j < numFeatures; j++) {
//          LOG.info("Rating index " + ratingIndex);
          if (j < fixedMatrix.get(ratingIndex).size()) {
            conf[j] = fixedMatrix.get(ratingIndex).get(j);
          } else {
            conf[j] = 0.0;
          }
        }

        NETLIB_BLAS.dspr("U", numFeatures, 1.0, ArrayUtils.toPrimitive(conf), 1,
            ArrayUtils.toPrimitive(upperTriangularLeftMatrix));
        if (rating != 0.0) {
          NETLIB_BLAS.daxpy(numFeatures, rating, ArrayUtils.toPrimitive(conf), 1, rightSideVector, 1);
        }
      }

      final Double regParam = lambda * size;
      Integer a = 0;
      Integer b = 2;
      while (a < upperTriangularLeftMatrix.length) {
        upperTriangularLeftMatrix[a] += regParam;
        a += b;
        b += 1;
      }

      final intW info = new intW(0);

      NETLIB_LAPACK.dppsv("U", numFeatures, 1, ArrayUtils.toPrimitive(upperTriangularLeftMatrix),
          rightSideVector, numFeatures, info);
      if (info.val != 0) {
        throw new RuntimeException("returned info value : " + info.val);
      }

      for (Integer i = 0; i < numFeatures; i++) {
        vector.add(i, rightSideVector[i]);
      }

      results.add(KV.of(c.element().getKey(), vector));
    }

    /**
     * FinishBundle method for BEAM.
     *
     * @param c Context.
     */
    @FinishBundle
    public void finishBundle(final FinishBundleContext c) {
      results.forEach(r -> c.output(r, null, null));
    }
  }

  /**
   * Composite transform that wraps the transforms inside the loop.
   * The loop updates the user matrix and the item matrix in each iteration.
   */
  public static final class UpdateUserAndItemMatrix extends LoopCompositeTransform<
      PCollection<KV<Integer, List<Double>>>, PCollection<KV<Integer, List<Double>>>> {
    private final Integer numFeatures;
    private final Double lambda;
    private final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedUserData;
    private final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedItemData;

    /**
     * Constructor of UpdateUserAndItemMatrix CompositeTransform.
     * @param numFeatures number of features.
     * @param lambda lambda.
     * @param parsedUserData PCollection of parsed user data.
     * @param parsedItemData PCollection of parsed item data.
     */
    UpdateUserAndItemMatrix(final Integer numFeatures, final Double lambda,
                            final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedUserData,
                            final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedItemData) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.parsedUserData = parsedUserData;
      this.parsedItemData = parsedItemData;
    }

    @Override
    public PCollection<KV<Integer, List<Double>>> expand(final PCollection<KV<Integer, List<Double>>> itemMatrix) {
      // Make Item Matrix view.
      final PCollectionView<Map<Integer, List<Double>>> itemMatrixView =
          itemMatrix.apply(GroupByKey.create()).apply(ParDo.of(new UngroupSingleVectorList())).apply(View.asMap());

      // Get new User Matrix
      final PCollectionView<Map<Integer, List<Double>>> userMatrixView = parsedUserData
          .apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, itemMatrixView)).withSideInputs(itemMatrixView))
          .apply(GroupByKey.create()).apply(ParDo.of(new UngroupSingleVectorList())).apply(View.asMap());
      // return new Item Matrix
      return parsedItemData.apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, userMatrixView))
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
    if (args.length > 3) {
      lambda = Double.parseDouble(args[3]);
    } else {
      lambda = 0.05;
    }
    final String outputFilePath;
    boolean checkOutput = false;
    if (args.length > 4) {
      outputFilePath = args[4];
      checkOutput = true;
    } else {
      outputFilePath = "";
    }

    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("ALS");
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);

    final Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(Pair.class, PairCoder.class));

    // Read raw data
    final PCollection<String> rawData = GenericSourceSink.read(p, inputFilePath);

    // Parse data for item
    final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedItemData = rawData
        .apply(ParDo.of(new ParseLine(false)))
        .apply(Combine.perKey(new TrainingDataCombiner()));

    // Parse data for user
    final PCollection<KV<Integer, Pair<List<Integer>, List<Double>>>> parsedUserData = rawData
        .apply(ParDo.of(new ParseLine(true)))
        .apply(Combine.perKey(new TrainingDataCombiner()));

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
      itemMatrix = itemMatrix.apply(new UpdateUserAndItemMatrix(numFeatures, lambda, parsedUserData, parsedItemData));
    }

    if (checkOutput) {
      final PCollection<String> result = itemMatrix.apply(MapElements.<KV<Integer, List<Double>>, String>via(
          new SimpleFunction<KV<Integer, List<Double>>, String>() {
            @Override
            public String apply(final KV<Integer, List<Double>> elem) {
              final List<String> values = elem.getValue().stream().map(e -> e.toString()).collect(Collectors.toList());
              return elem.getKey() + "," + String.join(",", values);
            }
          }));

      GenericSourceSink.write(result, outputFilePath);
    }

    p.run();
    LOG.info("JCT " + (System.currentTimeMillis() - start));
  }
}

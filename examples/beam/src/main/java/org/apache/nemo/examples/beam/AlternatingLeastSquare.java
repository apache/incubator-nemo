/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.examples.beam;

import com.github.fommil.netlib.BLAS;
import com.github.fommil.netlib.LAPACK;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang.ArrayUtils;
import org.apache.nemo.compiler.frontend.beam.transform.LoopCompositeTransform;
import org.netlib.util.intW;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  public static final class ParseLine extends DoFn<String, KV<Integer, KV<int[], float[]>>> {
    private final boolean isUserData;

    /**
     * Constructor for Parseline DoFn class.
     *
     * @param isUserData flag that distinguishes user data from item data.
     */
    public ParseLine(final boolean isUserData) {
      this.isUserData = isUserData;
    }

    /**
     * ProcessElement method for BEAM.
     *
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
      final int userId = Integer.parseInt(split[0]);
      final int itemId = Integer.parseInt(split[1]);
      final float rating = Float.parseFloat(split[2]);


      final int[] userAry = new int[1];
      userAry[0] = userId;

      final int[] itemAry = new int[1];
      itemAry[0] = itemId;

      final float[] ratingAry = new float[1];
      ratingAry[0] = rating;
      if (isUserData) {
        c.output(KV.of(userId, KV.of(itemAry, ratingAry)));
      } else {
        c.output(KV.of(itemId, KV.of(userAry, ratingAry)));
      }
    }
  }

  /**
   * A DoFn that relays a single vector list.
   */
  public static final class UngroupSingleVectorList
    extends DoFn<KV<Integer, Iterable<float[]>>, KV<Integer, float[]>> {

    /**
     * ProcessElement method for BEAM.
     *
     * @param c ProcessContext.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final KV<Integer, Iterable<float[]>> element = c.element();
      final Iterator<float[]> floatIterator = element.getValue().iterator();
      final float[] floatList = floatIterator.next();

      if (floatIterator.hasNext()) {
        throw new RuntimeException("Only a single vector list is expected");
      }

      // Output the ungrouped single vector list
      c.output(KV.of(element.getKey(), floatList));
    }
  }

  /**
   * Combiner for the training data.
   */
  public static final class TrainingDataCombiner
    extends Combine.CombineFn<KV<int[], float[]>, List<KV<int[], float[]>>, KV<int[], float[]>> {

    @Override
    public List<KV<int[], float[]>> createAccumulator() {
      return new LinkedList<>();
    }

    @Override
    public List<KV<int[], float[]>> addInput(final List<KV<int[], float[]>> accumulator,
                                             final KV<int[], float[]> value) {
      accumulator.add(value);
      return accumulator;
    }

    @Override
    public List<KV<int[], float[]>> mergeAccumulators(final Iterable<List<KV<int[], float[]>>> accumulators) {
      final List<KV<int[], float[]>> merged = new LinkedList<>();
      for (final List<KV<int[], float[]>> acc : accumulators) {
        merged.addAll(acc);
      }
      return merged;
    }

    @Override
    public KV<int[], float[]> extractOutput(final List<KV<int[], float[]>> accumulator) {
      int dimension = 0;
      for (final KV<int[], float[]> kv : accumulator) {
        dimension += kv.getKey().length;
      }

      final int[] intArr = new int[dimension];
      final float[] floatArr = new float[dimension];

      int itr = 0;
      for (final KV<int[], float[]> kv : accumulator) {
        final int[] ints = kv.getKey();
        final float[] floats = kv.getValue();
        for (int i = 0; i < ints.length; i++) {
          intArr[itr] = ints[i];
          floatArr[itr] = floats[i];
          itr++;
        }
      }

      return KV.of(intArr, floatArr);
    }
  }

  /**
   * DoFn for calculating next matrix at each iteration.
   */
  public static final class CalculateNextMatrix extends DoFn<KV<Integer, KV<int[], float[]>>, KV<Integer, float[]>> {
    private static final LAPACK NETLIB_LAPACK = LAPACK.getInstance();
    private static final BLAS NETLIB_BLAS = BLAS.getInstance();

    private final int numFeatures;
    private final double lambda;
    private final PCollectionView<Map<Integer, float[]>> fixedMatrixView;

    /**
     * Constructor for CalculateNextMatrix DoFn class.
     *
     * @param numFeatures     number of features.
     * @param lambda          lambda.
     * @param fixedMatrixView a PCollectionView of the fixed matrix (item / user matrix).
     */
    public CalculateNextMatrix(final int numFeatures,
                               final double lambda,
                               final PCollectionView<Map<Integer, float[]>> fixedMatrixView) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.fixedMatrixView = fixedMatrixView;
    }

    /**
     * ProcessElement method for BEAM.
     *
     * @param c ProcessContext.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final double[] upperTriangularLeftMatrix = new double[numFeatures * (numFeatures + 1) / 2];
      final Map<Integer, float[]> fixedMatrix = c.sideInput(fixedMatrixView);

      final int[] indexArr = c.element().getValue().getKey();
      final float[] ratingArr = c.element().getValue().getValue();

      final int size = indexArr.length;

      final float[] vector = new float[numFeatures];
      final double[] rightSideVector = new double[numFeatures];
      final double[] tmp = new double[numFeatures];
      for (int i = 0; i < size; i++) {
        final int ratingIndex = indexArr[i];
        final float rating = ratingArr[i];
        for (int j = 0; j < numFeatures; j++) {
          tmp[j] = fixedMatrix.get(ratingIndex)[j];
        }
        NETLIB_BLAS.dspr("U", numFeatures, 1.0, tmp, 1, upperTriangularLeftMatrix);
        if (rating != 0.0) {
          NETLIB_BLAS.daxpy(numFeatures, rating, tmp, 1, rightSideVector, 1);
        }
      }

      final double regParam = lambda * size;
      int a = 0;
      int b = 2;
      while (a < upperTriangularLeftMatrix.length) {
        upperTriangularLeftMatrix[a] += regParam;
        a += b;
        b += 1;
      }

      final intW info = new intW(0);

      NETLIB_LAPACK.dppsv("U", numFeatures, 1, upperTriangularLeftMatrix, rightSideVector, numFeatures, info);
      if (info.val != 0) {
        throw new RuntimeException("returned info value : " + info.val);
      }

      for (int i = 0; i < vector.length; i++) {
        vector[i] = (float) rightSideVector[i];
      }

      c.output(KV.of(c.element().getKey(), vector));
    }
  }

  /**
   * Composite transform that wraps the transforms inside the loop.
   * The loop updates the user matrix and the item matrix in each iteration.
   */
  public static final class UpdateUserAndItemMatrix
    extends LoopCompositeTransform<PCollection<KV<Integer, float[]>>, PCollection<KV<Integer, float[]>>> {
    private final Integer numFeatures;
    private final double lambda;
    private final PCollection<KV<Integer, KV<int[], float[]>>> parsedUserData;
    private final PCollection<KV<Integer, KV<int[], float[]>>> parsedItemData;

    /**
     * Constructor of UpdateUserAndItemMatrix CompositeTransform.
     *
     * @param numFeatures    number of features.
     * @param lambda         lambda.
     * @param parsedUserData PCollection of parsed user data.
     * @param parsedItemData PCollection of parsed item data.
     */
    UpdateUserAndItemMatrix(final Integer numFeatures, final double lambda,
                            final PCollection<KV<Integer, KV<int[], float[]>>> parsedUserData,
                            final PCollection<KV<Integer, KV<int[], float[]>>> parsedItemData) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.parsedUserData = parsedUserData;
      this.parsedItemData = parsedItemData;
    }

    @Override
    public PCollection<KV<Integer, float[]>> expand(final PCollection<KV<Integer, float[]>> itemMatrix) {
      // Make Item Matrix view.
      final PCollectionView<Map<Integer, float[]>> itemMatrixView =
        itemMatrix.apply(GroupByKey.create()).apply(ParDo.of(new UngroupSingleVectorList())).apply(View.asMap());

      // Get new User Matrix
      final PCollectionView<Map<Integer, float[]>> userMatrixView = parsedUserData
        .apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, itemMatrixView)).withSideInputs(itemMatrixView))
        .apply(GroupByKey.create()).apply(ParDo.of(new UngroupSingleVectorList())).apply(View.asMap());

      // return new Item Matrix
      return parsedItemData.apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, userMatrixView))
        .withSideInputs(userMatrixView));
    }
  }

  /**
   * A DoFn that creates an initial matrix.
   */
  public static final class CreateInitialMatrix extends DoFn<KV<Integer, KV<int[], float[]>>, KV<Integer, float[]>> {
    private final int numFeatures;
    private final boolean isDeterministic;

    /**
     * @param numFeatures     number of the features.
     * @param isDeterministic whether or not to initialize the matrix in deterministic mode.
     */
    CreateInitialMatrix(final int numFeatures,
                        final boolean isDeterministic) {
      this.numFeatures = numFeatures;
      this.isDeterministic = isDeterministic;
    }

    /**
     * ProcessElement method for BEAM.
     *
     * @param c ProcessContext.
     * @throws Exception Exception on the way.
     */
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
        if (isDeterministic) {
          result[i] = (float) (0.5 * 0.01); // use a deterministic average value
        } else {
          result[i] = (float) (Math.random() * 0.01);
        }
      }
      c.output(KV.of(element.getKey(), result));
    }
  }

  /**
   * Main function for the ALS BEAM program.
   *
   * @param args arguments.
   * @throws ClassNotFoundException exception.
   */
  public static void main(final String[] args) throws ClassNotFoundException {
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

    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("ALS");
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);

    final Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(int[].class, IntArrayCoder.class));
    p.getCoderRegistry().registerCoderProvider(CoderProviders.fromStaticMethods(float[].class, FloatArrayCoder.class));

    // Read raw data
    final PCollection<String> rawData = GenericSourceSink.read(p, inputFilePath);

    // Parse data for item
    final PCollection<KV<Integer, KV<int[], float[]>>> parsedItemData = rawData
      .apply(ParDo.of(new ParseLine(false)))
      .apply(Combine.perKey(new TrainingDataCombiner()));

    // Parse data for user
    final PCollection<KV<Integer, KV<int[], float[]>>> parsedUserData = rawData
      .apply(ParDo.of(new ParseLine(true)))
      .apply(Combine.perKey(new TrainingDataCombiner()));

    // Create Initial Item Matrix
    PCollection<KV<Integer, float[]>> itemMatrix =
      parsedItemData.apply(ParDo.of(new CreateInitialMatrix(numFeatures, checkOutput)));

    // Iterations to update Item Matrix.
    for (int i = 0; i < numItr; i++) {
      // NOTE: a single composite transform for the iteration.
      itemMatrix = itemMatrix.apply(new UpdateUserAndItemMatrix(numFeatures, lambda, parsedUserData, parsedItemData));
    }

    if (checkOutput) {
      final PCollection<String> result = itemMatrix.apply(MapElements.<KV<Integer, float[]>, String>via(
        new SimpleFunction<KV<Integer, float[]>, String>() {
          @Override
          public String apply(final KV<Integer, float[]> elem) {
            final List<String> values = Stream.of(ArrayUtils.toObject(elem.getValue()))
              .map(String::valueOf)
              .collect(Collectors.toList());
            return elem.getKey() + "," + String.join(",", values);
          }
        }));

      GenericSourceSink.write(result, outputFilePath);
    }

    p.run();
    LOG.info("JCT " + (System.currentTimeMillis() - start));
  }
}

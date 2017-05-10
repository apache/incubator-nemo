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

import com.github.fommil.netlib.BLAS;
import com.github.fommil.netlib.LAPACK;
import edu.snu.vortex.client.beam.LoopCompositeTransform;
import edu.snu.vortex.compiler.frontend.beam.Runner;
import edu.snu.vortex.compiler.frontend.beam.coder.PairCoder;
import edu.snu.vortex.utils.Pair;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.netlib.util.intW;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sample Alternating Least Square application.
 */
public final class AlternatingLeastSquare {
  private static final Logger LOG = Logger.getLogger(AlternatingLeastSquare.class.getName());

  private AlternatingLeastSquare() {
  }

  /**
   * Method for parsing the input line.
   */
  public static final class ParseLine extends DoFn<String, KV<Integer, Pair<int[], float[]>>> {
    private final boolean isUserData;

    public ParseLine(final boolean isUserData) {
      this.isUserData = isUserData;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final String text = ((String) c.element()).trim();
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
        c.output(KV.of(userId, Pair.of(itemAry, ratingAry)));
      } else {
        c.output(KV.of(itemId, Pair.of(userAry, ratingAry)));
      }
    }
  }

  /**
   * Combiner for the training data.
   */
  public static final class TrainingDataCombiner extends
      Combine.KeyedCombineFn<Integer, Pair<int[], float[]>, List<Pair<int[], float[]>>, Pair<int[], float[]>> {

    @Override
    public List<Pair<int[], float[]>> createAccumulator(final Integer key) {
      return new LinkedList<>();
    }

    @Override
    public List<Pair<int[], float[]>> addInput(final Integer key,
                                               final List<Pair<int[], float[]>> accumulator,
                                               final Pair<int[], float[]> value) {
      accumulator.add(value);
      return accumulator;
    }

    @Override
    public List<Pair<int[], float[]>> mergeAccumulators(final Integer key,
                                                        final Iterable<List<Pair<int[], float[]>>> accumulators) {
      final List<Pair<int[], float[]>> merged = new LinkedList<>();
      accumulators.forEach(merged::addAll);
      return merged;
    }

    @Override
    public Pair<int[], float[]> extractOutput(final Integer key,
                                              final List<Pair<int[], float[]>> accumulator) {
      int dimension = 0;
      for (final Pair<int[], float[]> pair : accumulator) {
        dimension += pair.left().length;
      }

      final int[] intArr = new int[dimension];
      final float[] floatArr = new float[dimension];

      int itr = 0;
      for (final Pair<int[], float[]> pair : accumulator) {
        final int[] ints = pair.left();
        final float[] floats = pair.right();
        for (int i = 0; i < ints.length; i++) {
          intArr[itr] = ints[i];
          floatArr[itr] = floats[i];
          itr++;
        }
      }

      return Pair.of(intArr, floatArr);
    }
  }

  /**
   * DoFn for calculating next matrix at each iteration.
   */
  public static final class CalculateNextMatrix extends DoFn<KV<Integer, Pair<int[], float[]>>, KV<Integer, float[]>> {
    private static final LAPACK NETLIB_LAPACK = LAPACK.getInstance();
    private static final BLAS NETLIB_BLAS = BLAS.getInstance();

    private final List<KV<Integer, float[]>> results;
    private final double[] upperTriangularLeftMatrix;
    private final int numFeatures;
    private final double lambda;
    private final PCollectionView<Map<Integer, float[]>> fixedMatrixView;

    CalculateNextMatrix(final int numFeatures, final double lambda,
                        final PCollectionView<Map<Integer, float[]>> fixedMatrixView) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.fixedMatrixView = fixedMatrixView;
      this.results = new LinkedList<>();
      this.upperTriangularLeftMatrix = new double[numFeatures * (numFeatures + 1) / 2];
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      for (int j = 0; j < upperTriangularLeftMatrix.length; j++) {
        upperTriangularLeftMatrix[j] = 0.0;
      }

      final Map<Integer, float[]> fixedMatrix = c.sideInput(fixedMatrixView);

      final int[] indexArr = c.element().getValue().left();
      final float[] ratingArr = c.element().getValue().right();

      final int size = indexArr.length;

      final float[] vector = new float[numFeatures];
      final double[] rightSideVector = new double[numFeatures];
      final double[] tmp = new double[numFeatures];
      for (int i = 0; i < size; i++) {
        final int ratingIndex = indexArr[i];
        final float rating = ratingArr[i];
        for (int j = 0; j < numFeatures; j++) {
//          LOG.log(Level.INFO, "Rating index " + ratingIndex);
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

      results.add(KV.of(c.element().getKey(), vector));
    }

    @FinishBundle
    public void finishBundle(final Context c) {
      results.forEach(c::output);
    }
  }

  /**
   * Composite transform that wraps the transforms inside the loop.
   * The loop updates the user matrix and the item matrix in each iteration.
   */
  public static final class UpdateUserAndItemMatrix extends LoopCompositeTransform<
      PCollection<KV<Integer, float[]>>, PCollection<KV<Integer, float[]>>> {
    private final int numFeatures;
    private final double lambda;
    private final PCollection<KV<Integer, Pair<int[], float[]>>> parsedUserData;
    private final PCollection<KV<Integer, Pair<int[], float[]>>> parsedItemData;

    UpdateUserAndItemMatrix(final int numFeatures, final double lambda,
                            final PCollection<KV<Integer, Pair<int[], float[]>>> parsedUserData,
                            final PCollection<KV<Integer, Pair<int[], float[]>>> parsedItemData) {
      this.numFeatures = numFeatures;
      this.lambda = lambda;
      this.parsedUserData = parsedUserData;
      this.parsedItemData = parsedItemData;
    }

    @Override
    public PCollection<KV<Integer, float[]>> expand(final PCollection<KV<Integer, float[]>> itemMatrix) {
      // Make Item Matrix view.
      final PCollectionView<Map<Integer, float[]>> itemMatrixView = itemMatrix.apply(View.asMap());
      // Get new User Matrix
      final PCollectionView<Map<Integer, float[]>> userMatrixView = parsedUserData
          .apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, itemMatrixView)).withSideInputs(itemMatrixView))
          .apply(View.asMap());
      // return new Item Matrix
      return parsedItemData.apply(ParDo.of(new CalculateNextMatrix(numFeatures, lambda, userMatrixView))
          .withSideInputs(userMatrixView));
    }
  }

  public static void main(final String[] args) {
    final long start = System.currentTimeMillis();
    LOG.log(Level.INFO, Arrays.toString(args));
    final String inputFilePath = args[0];
    final int numFeatures = Integer.parseInt(args[1]);
    final int numItr = Integer.parseInt(args[2]);
    final double lambda;
    if (args.length > 4) {
      lambda = Double.parseDouble(args[3]);
    } else {
      lambda = 0.05;
    }

    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);
    options.setJobName("ALS");

    final Pipeline p = Pipeline.create(options);
    p.getCoderRegistry().registerCoder(Pair.class, PairCoder.class);

    // Read raw data
    final PCollection<String> rawData = p.apply(TextIO.Read.from(inputFilePath));

    // Parse data for item
    final PCollection<KV<Integer, Pair<int[], float[]>>> parsedItemData = rawData
        .apply(ParDo.of(new ParseLine(false)))
        .apply(Combine.perKey(new TrainingDataCombiner()));

    // Parse data for user
    final PCollection<KV<Integer, Pair<int[], float[]>>> parsedUserData = rawData
        .apply(ParDo.of(new ParseLine(true)))
        .apply(Combine.perKey(new TrainingDataCombiner()));

    // Create Initial Item Matrix
    PCollection<KV<Integer, float[]>> itemMatrix = parsedItemData
        .apply(ParDo.of(new DoFn<KV<Integer, Pair<int[], float[]>>, KV<Integer, float[]>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) throws Exception {
            final float[] result = new float[numFeatures];

            final KV<Integer, Pair<int[], float[]>> element = c.element();
            final float[] ratings = element.getValue().right();
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
    for (int i = 0; i < numItr; i++) {
      // NOTE: a single composite transform for the iteration.
      itemMatrix = itemMatrix.apply(new UpdateUserAndItemMatrix(numFeatures, lambda, parsedUserData, parsedItemData));
    }

    p.run();
    LOG.log(Level.INFO, "JCT " + (System.currentTimeMillis() - start));
  }
}

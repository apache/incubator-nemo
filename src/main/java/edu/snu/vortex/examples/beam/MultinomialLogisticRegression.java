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
import edu.snu.vortex.compiler.frontend.beam.Runner;
import edu.snu.vortex.utils.Pair;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sample Multinomial Logistic Regression application.
 */
public final class MultinomialLogisticRegression {
  private static final Logger LOG = Logger.getLogger(MultinomialLogisticRegression.class.getName());

  private MultinomialLogisticRegression() {
  }

  /**
   * Calculate Gradient.
   */
  public static final class CalculateGradient extends DoFn<String, KV<Integer, double[]>> {
    private double[][] gradients;
    private final int numClasses;
    private final int numFeatures;
    private final PCollectionView<Map<Integer, double[]>> modelView;
    private Map<Integer, double[]> model;

    CalculateGradient(final PCollectionView<Map<Integer, double[]>> modelView,
                      final int numClasses,
                      final int numFeatures) {
      this.modelView = modelView;
      this.numClasses = numClasses;
      this.numFeatures = numFeatures;
      this.gradients = null;
    }

    private void initializeGradients() {
      this.gradients = new double[this.numClasses][];
      for (int i = 0; i < this.numClasses - 1; i++) {
        gradients[i] = new double[numFeatures + 1];
      }
      gradients[this.numClasses - 1] = new double[3];
    }

    private KV<Integer, Pair<int[], double[]>> parseLine(final String input) {
      final String text = input.trim();
      if (text.startsWith("#") || text.length() == 0) { // comments or newline
        return null;
      }

      final String[] split = text.split("\\s+|:");
      final int output = Integer.parseInt(split[0]);

      final int[] indices = new int[split.length / 2];
      final double[] data = new double[split.length / 2];
      for (int index = 0; index < split.length / 2; ++index) {
        indices[index] = Integer.parseInt(split[2 * index + 1]) - 1;
        data[index] = Double.parseDouble(split[2 * index + 2]);
      }

      return KV.of(output, Pair.of(indices, data));
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final KV<Integer, Pair<int[], double[]>> data = parseLine(c.element());
      if (data == null) { // comments and newlines
        return;
      }

      if (gradients == null) {
        initializeGradients();
      }

      this.model = c.sideInput(modelView);
      final int label = data.getKey();
      final int[] indices = data.getValue().left();
      final double[] features = data.getValue().right();

      final double[] margins = new double[numClasses - 1];
      double marginY = 0.0;
      double maxMargin = -Double.MAX_VALUE;
      int maxMarginIndex = 0;

      for (int i = 0; i < numClasses - 1; i++) {
        final double[] weightArr = model.get(i);
        double margin = 0.0;
        for (int indexItr = 0; indexItr < indices.length; indexItr++) {
          final int index = indices[indexItr];
          final double value = features[indexItr];
          if (value != 0.0) {
            margin += value * weightArr[index];
          }
        }

        if (i == label - 1) {
          marginY = margin;
        }

        if (margin > maxMargin) {
          maxMargin = margin;
          maxMarginIndex = i;
        }

        margins[i] = margin;
      }


      /**
       * When maxMargin > 0, the original formula will cause overflow as we discuss
       * in the previous comment.
       * We address this by subtracting maxMargin from all the margins, so it's guaranteed
       * that all of the new margins will be smaller than zero to prevent arithmetic overflow.
       */
      double sum = 0.0;
      if (maxMargin > 0) {
        for (int i = 0; i < numClasses - 1; i++) {
          margins[i] -= maxMargin;
          if (i == maxMarginIndex) {
            sum += Math.exp(-maxMargin);
          } else {
            sum += Math.exp(margins[i]);
          }
        }
      } else {
        for (int i = 0; i < numClasses - 1; i++) {
          sum += Math.exp(margins[i]);
        }
      }

      for (int i = 0; i < numClasses - 1; i++) {
        double multiplier = Math.exp(margins[i]) / (sum + 1.0);
        if (label != 0 && label == i + 1) {
          multiplier -= 1;
        }

        final double[] gradientArr = gradients[i];

        for (int indexItr = 0; indexItr < indices.length; indexItr++) {
          final int index = indices[indexItr];
          final double value = features[indexItr];
          if (value != 0.0) {
            gradientArr[index] += multiplier * value;
          }
        }

        gradientArr[numFeatures] += 1;
      }

      double partialLoss;
      if (label > 0) {
        partialLoss = Math.log1p(sum) - marginY;
      } else {
        partialLoss = Math.log1p(sum);
      }

      if (maxMargin > 0) {
        partialLoss += maxMargin;
      }

      gradients[numClasses - 1][0] += 1;
      gradients[numClasses - 1][2] += partialLoss;
    }

    @FinishBundle
    public void finishBundle(final Context context) {
      for (int i = 0; i < gradients.length; i++) {
        context.output(KV.of(i, gradients[i]));
      }
      LOG.log(Level.INFO, "stats: " + Arrays.toString(gradients[numClasses - 1]));
    }
  }

  /**
   * DoFn class that applies the gradient to the model.
   */
  public static final class ApplyGradient extends DoFn<KV<Integer, CoGbkResult>, KV<Integer, double[]>> {
    private final TupleTag<double[]> gradientTag;
    private final TupleTag<double[]> modelTag;
    private final int numFeatures;
    private final int numClasses;
    private final int iterationNum;

    ApplyGradient(final int numFeatures, final int numClasses, final int iterationNum,
                  final TupleTag<double[]> gradientTag, final TupleTag<double[]> modelTag) {
      this.numFeatures = numFeatures;
      this.numClasses = numClasses;
      this.iterationNum = iterationNum;
      this.gradientTag = gradientTag;
      this.modelTag = modelTag;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final KV<Integer, CoGbkResult> kv = c.element();
      final double[] gradientArr = kv.getValue().getOnly(gradientTag);
      final double[] prevModelArr = kv.getValue().getOnly(modelTag);
      final double[] gradient;
      final double[] prevModel;
      if (gradientArr.length > prevModelArr.length) {
        gradient = gradientArr;
        prevModel = prevModelArr;
      } else {
        gradient = prevModelArr;
        prevModel = gradientArr;
      }

      if (kv.getKey() == numClasses - 1) {
        final int numData = (int) gradient[0];
        final double lossSum = gradient[2];
        LOG.log(Level.INFO, "[" + iterationNum + "-th] Num Data: " + numData + " Loss : " + lossSum / numData);
        c.output(KV.of(kv.getKey(), prevModel));
      } else {
        final int numData = (int) gradient[numFeatures];
        final double stepSize = 1.0 / Math.sqrt(iterationNum);
        final double multiplier = stepSize / numData;

        final double[] ret = new double[prevModel.length];
        for (int i = 0; i < prevModel.length; i++) {
          ret[i] = prevModel[i] - multiplier * gradient[i];
        }
        c.output(KV.of(kv.getKey(), ret));
      }
    }

    @FinishBundle
    public void finishBundle(final Context context) {
    }
  }

  /**
   * Combine Function for two double arrays.
   */
  public static final class CombineFunction extends Combine.BinaryCombineFn<double[]> {
    @Override
    public double[] apply(final double[] left, final double[] right) {
      for (int i = 0; i < left.length; i++) {
        left[i] += right[i];
      }
      return left;
    }
  }

  /**
   * Combine Function for Iterable of gradients.
   */
  public static final class CombineFunctionForIterable implements SerializableFunction<Iterable<double[]>, double[]> {
    @Override
    public double[] apply(final Iterable<double[]> gradients) {
      double[] ret = null;
      for (final double[] gradient : gradients) {
        if (ret == null) { // initialize
          ret = new double[gradient.length];
        }

        for (int i = 0; i < ret.length; i++) {
          ret[i] += gradient[i];
        }
      }

      return ret;
    }
  }

  /**
   + Composite transform that wraps the transforms inside the loop.
   + The loop updates the model in each iteration.
   */
  public static final class UpdateModel
      extends LoopCompositeTransform<PCollection<KV<Integer, double[]>>, PCollection<KV<Integer, double[]>>> {
    private final int numFeatures;
    private final int numClasses;
    private final int iterationNum;
    private final PCollection<String> readInput;

    UpdateModel(final int numFeatures, final int numClasses, final int iterationNum,
                final PCollection<String> readInput) {
      this.numFeatures = numFeatures;
      this.numClasses = numClasses;
      this.iterationNum = iterationNum;
      this.readInput = readInput;
    }

    @Override
    public PCollection<KV<Integer, double[]>> expand(final PCollection<KV<Integer, double[]>> model) {
      // Model as a view.
      final PCollectionView<Map<Integer, double[]>> modelView = model.apply(View.asMap());

      // Find gradient.
      final PCollection<KV<Integer, double[]>> gradient = readInput
          .apply(ParDo.of(
              new CalculateGradient(modelView, numClasses, numFeatures)).withSideInputs(modelView))
          .apply(Combine.perKey(new CombineFunction()));

      // Tags for CoGroupByKey.
      final TupleTag<double[]> gradientTag = new TupleTag<>();
      final TupleTag<double[]> modelTag = new TupleTag<>();
      final KeyedPCollectionTuple<Integer> coGbkInput = KeyedPCollectionTuple
          .of(gradientTag, gradient)
          .and(modelTag, model);

      final PCollection<KV<Integer, CoGbkResult>> groupResult =
          coGbkInput.apply(CoGroupByKey.create());

      // Update the model
      return groupResult
          .apply(ParDo.of(new ApplyGradient(numFeatures, numClasses, iterationNum, gradientTag, modelTag)));
    }
  }

  public static void main(final String[] args) {
    final long start = System.currentTimeMillis();
    LOG.log(Level.INFO, Arrays.toString(args));
    final String inputFilePath = args[0];
    final int numFeatures = Integer.parseInt(args[1]);
    final int numClasses = Integer.parseInt(args[2]);
    final int numItr = Integer.parseInt(args[3]);

    final double lambda = 0.0;

    final List<Integer> initialModelKeys = new ArrayList<>(numClasses);
    for (int i = 0; i < numClasses; i++) {
      initialModelKeys.add(i);
    }

    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(Runner.class);
    options.setJobName("MLR");

    final Pipeline p = Pipeline.create(options);

    // Initialization of the model for Logistic Regression.
    PCollection<KV<Integer, double[]>> model = p
        .apply(Create.of(initialModelKeys))
        .apply(ParDo.of(new DoFn<Integer, KV<Integer, double[]>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) throws Exception {
            if (c.element() == numClasses - 1) {
              c.output(KV.of(c.element(), new double[1]));
            } else {
              final double[] model = new double[numFeatures];
              c.output(KV.of(c.element(), model));
            }
          }
        }));

    // Read input data
    final PCollection<String> readInput = p
        .apply(TextIO.Read.from(inputFilePath));

    // Multiple iterations for convergence.
    for (int i = 1; i <= numItr; i++) {
      // NOTE: a single composite transform for the iteration.
      model = model.apply(new UpdateModel(numFeatures, numClasses, i, readInput));
    }

    p.run();
    LOG.log(Level.INFO, "JCT " + (System.currentTimeMillis() - start));
  }
}

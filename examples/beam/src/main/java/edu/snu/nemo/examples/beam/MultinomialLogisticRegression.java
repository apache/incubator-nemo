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
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
import edu.snu.nemo.common.Pair;
import org.apache.beam.sdk.Pipeline;
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

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Multinomial Logistic Regression application.
 */
public final class MultinomialLogisticRegression {
  private static final Logger LOG = LoggerFactory.getLogger(MultinomialLogisticRegression.class.getName());

  /**
   * Private constructor.
   */
  private MultinomialLogisticRegression() {
  }

  /**
   * Calculate Gradient.
   */
  public static final class CalculateGradient extends DoFn<String, KV<Integer, List<Double>>> {
    private List<List<Double>> gradients;
    private final Integer numClasses;
    private final Integer numFeatures;
    private final PCollectionView<Map<Integer, List<Double>>> modelView;
    private Map<Integer, List<Double>> model;

    /**
     * Constructor for CalculateGradient DoFn class.
     * @param modelView PCollectionView of the model.
     * @param numClasses number of classes.
     * @param numFeatures number of features.
     */
    CalculateGradient(final PCollectionView<Map<Integer, List<Double>>> modelView,
                      final Integer numClasses,
                      final Integer numFeatures) {
      this.modelView = modelView;
      this.numClasses = numClasses;
      this.numFeatures = numFeatures;
      this.gradients = null;
    }

    /**
     * Initialization of gradients.
     */
    private void initializeGradients() {
      this.gradients = new ArrayList<>(this.numClasses);
      for (Integer i = 0; i < this.numClasses - 1; i++) {
        gradients.add(i, new ArrayList<>(numFeatures + 1));
        for (Integer j = 0; j < numFeatures + 1; j++) {
          gradients.get(i).add(j, 0.0);
        }
      }
      gradients.add(this.numClasses - 1, new ArrayList<>(3));
      for (Integer i = 0; i < 3; i++) {
        gradients.get(this.numClasses - 1).add(i, 0.0);
      }
    }

    /**
     * Method for parsing lines of inputs.
     * @param input input line.
     * @return the parsed key-value pair.
     */
    private KV<Integer, Pair<List<Integer>, List<Double>>> parseLine(final String input) {
      final String text = input.trim();
      if (text.startsWith("#") || text.length() == 0) { // comments or newline
        return null;
      }

      final String[] split = text.split("\\s+|:");
      final Integer output = Integer.parseInt(split[0]);

      final List<Integer> indices = new ArrayList<>(split.length / 2);
      final List<Double> data = new ArrayList<>(split.length / 2);
      for (Integer index = 0; index < split.length / 2; ++index) {
        indices.add(index, Integer.parseInt(split[2 * index + 1]) - 1);
        data.add(index, Double.parseDouble(split[2 * index + 2]));
      }

      return KV.of(output, Pair.of(indices, data));
    }

    /**
     * ProcessElement method for BEAM.
     * @param c Process context.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final KV<Integer, Pair<List<Integer>, List<Double>>> data = parseLine(c.element());
      if (data == null) { // comments and newlines
        return;
      }

      if (gradients == null) {
        initializeGradients();
      }

      this.model = c.sideInput(modelView);
      final Integer label = data.getKey();
      final List<Integer> indices = data.getValue().left();
      final List<Double> features = data.getValue().right();

      final List<Double> margins = new ArrayList<>(numClasses - 1);
      Double marginY = 0.0;
      Double maxMargin = -Double.MAX_VALUE;
      Integer maxMarginIndex = 0;

      for (Integer i = 0; i < numClasses - 1; i++) {
        final List<Double> weightArr = model.get(i);
        Double margin = 0.0;
        for (Integer indexItr = 0; indexItr < indices.size(); indexItr++) {
          final Integer index = indices.get(indexItr);
          final Double value = features.get(indexItr);
          if (value != 0.0) {
            margin += value * weightArr.get(index);
          }
        }

        if (i == label - 1) {
          marginY = margin;
        }

        if (margin > maxMargin) {
          maxMargin = margin;
          maxMarginIndex = i;
        }

        margins.add(i, margin);
      }


      /**
       * When maxMargin > 0, the original formula will cause overflow as we discuss
       * in the previous comment.
       * We address this by subtracting maxMargin from all the margins, so it's guaranteed
       * that all of the new margins will be smaller than zero to prevent arithmetic overflow.
       */
      Double sum = 0.0;
      if (maxMargin > 0) {
        for (Integer i = 0; i < numClasses - 1; i++) {
          margins.set(i, margins.get(i) - maxMargin);
          if (i == maxMarginIndex) {
            sum += Math.exp(-maxMargin);
          } else {
            sum += Math.exp(margins.get(i));
          }
        }
      } else {
        for (Integer i = 0; i < numClasses - 1; i++) {
          sum += Math.exp(margins.get(i));
        }
      }

      for (Integer i = 0; i < numClasses - 1; i++) {
        Double multiplier = Math.exp(margins.get(i)) / (sum + 1.0);
        if (label != 0 && label == i + 1) {
          multiplier -= 1;
        }

        final List<Double> gradientArr = gradients.get(i);

        for (Integer indexItr = 0; indexItr < indices.size(); indexItr++) {
          final Integer index = indices.get(indexItr);
          final Double value = features.get(indexItr);
          if (value != 0.0) {
            gradientArr.set(index, gradientArr.get(index) + (multiplier * value));
          }
        }

        gradientArr.set(numFeatures, gradientArr.get(numFeatures) + 1);
      }

      Double partialLoss;
      if (label > 0) {
        partialLoss = Math.log1p(sum) - marginY;
      } else {
        partialLoss = Math.log1p(sum);
      }

      if (maxMargin > 0) {
        partialLoss += maxMargin;
      }

      gradients.get(numClasses - 1).set(0, gradients.get(numClasses - 1).get(0) + 1);
      gradients.get(numClasses - 1).set(2, gradients.get(numClasses - 1).get(2) + partialLoss);
    }

    /**
     * FinishBundle method for BEAM.
     * @param c Context.
     */
    @FinishBundle
    public void finishBundle(final FinishBundleContext c) {
      for (Integer i = 0; i < gradients.size(); i++) {
        c.output(KV.of(i, gradients.get(i)), null, null);
      }
      LOG.info("stats: " + gradients.get(numClasses - 1).toString());
    }
  }

  /**
   * DoFn class that applies the gradient to the model.
   */
  public static final class ApplyGradient extends DoFn<KV<Integer, CoGbkResult>, KV<Integer, List<Double>>> {
    private final TupleTag<List<Double>> gradientTag;
    private final TupleTag<List<Double>> modelTag;
    private final Integer numFeatures;
    private final Integer numClasses;
    private final Integer iterationNum;

    /**
     * Constructor for ApplyGradient DoFn class.
     * @param numFeatures number of features.
     * @param numClasses number of classes.
     * @param iterationNum number of iteration.
     * @param gradientTag TupleTag of gradient.
     * @param modelTag TupleTag of model.
     */
    ApplyGradient(final Integer numFeatures, final Integer numClasses, final Integer iterationNum,
                  final TupleTag<List<Double>> gradientTag, final TupleTag<List<Double>> modelTag) {
      this.numFeatures = numFeatures;
      this.numClasses = numClasses;
      this.iterationNum = iterationNum;
      this.gradientTag = gradientTag;
      this.modelTag = modelTag;
    }

    /**
     * ProcessElement method for BEAM.
     * @param c Process context.
     * @throws Exception Exception on the way.
     */
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final KV<Integer, CoGbkResult> kv = c.element();
      final List<Double> gradientArr = kv.getValue().getOnly(gradientTag);
      final List<Double> prevModelArr = kv.getValue().getOnly(modelTag);
      final List<Double> gradient;
      final List<Double> prevModel;
      if (gradientArr.size() > prevModelArr.size()) {
        gradient = gradientArr;
        prevModel = prevModelArr;
      } else {
        gradient = prevModelArr;
        prevModel = gradientArr;
      }

      if (kv.getKey() == numClasses - 1) {
        final Integer numData = gradient.get(0).intValue();
        final Double lossSum = gradient.get(2);
        LOG.info("[" + iterationNum + "-th] Num Data: " + numData + " Loss : " + lossSum / numData);
        c.output(KV.of(kv.getKey(), prevModel));
      } else {
        final Integer numData = gradient.get(numFeatures).intValue();
        final Double stepSize = 1.0 / Math.sqrt(iterationNum);
        final Double multiplier = stepSize / numData;

        final List<Double> ret = new ArrayList<>(prevModel.size());
        for (Integer i = 0; i < prevModel.size(); i++) {
          ret.add(i, prevModel.get(i) - multiplier * gradient.get(i));
        }
        c.output(KV.of(kv.getKey(), ret));
      }
    }

    /**
     * FinishBundle method for BEAM.
     */
    @FinishBundle
    public void finishBundle() {
    }
  }

  /**
   * Combine Function for two Double arrays.
   */
  public static final class CombineFunction extends Combine.BinaryCombineFn<List<Double>> {
    @Override
    public List<Double> apply(final List<Double> left, final List<Double> right) {
      final Iterator<Double> leftItr = left.iterator();
      final Iterator<Double> rightItr = right.iterator();
      final List<Double> result = new ArrayList<>();
      while (leftItr.hasNext() && rightItr.hasNext()) {
        result.add(leftItr.next() + rightItr.next());
      }
      return result;
    }
  }

  /**
   * Combine Function for Iterable of gradients.
   */
  public static final class CombineFunctionForIterable
      implements SerializableFunction<Iterable<List<Double>>, List<Double>> {
    @Override
    public List<Double> apply(final Iterable<List<Double>> gradients) {
      List<Double> ret = null;
      for (final List<Double> gradient : gradients) {
        if (ret == null) { // initialize
          ret = new ArrayList<>(gradient.size());
          for (Integer i = 0; i < ret.size(); i++) {
            ret.add(i, 0.0);
          }
        }

        for (Integer i = 0; i < ret.size(); i++) {
          ret.set(i, ret.get(i) + gradient.get(i));
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
      extends LoopCompositeTransform<PCollection<KV<Integer, List<Double>>>, PCollection<KV<Integer, List<Double>>>> {
    private final Integer numFeatures;
    private final Integer numClasses;
    private final Integer iterationNum;
    private final PCollection<String> readInput;

    /**
     * Constructor of UpdateModel CompositeTransform.
     * @param numFeatures number of features.
     * @param numClasses number of classes.
     * @param iterationNum iteration number.
     * @param readInput PCollection of
     */
    UpdateModel(final Integer numFeatures, final Integer numClasses, final Integer iterationNum,
                final PCollection<String> readInput) {
      this.numFeatures = numFeatures;
      this.numClasses = numClasses;
      this.iterationNum = iterationNum;
      this.readInput = readInput;
    }

    @Override
    public PCollection<KV<Integer, List<Double>>> expand(final PCollection<KV<Integer, List<Double>>> model) {
      // Model as a view.
      final PCollectionView<Map<Integer, List<Double>>> modelView = model.apply(View.asMap());

      // Find gradient.
      final PCollection<KV<Integer, List<Double>>> gradient = readInput
          .apply(ParDo.of(
              new CalculateGradient(modelView, numClasses, numFeatures)).withSideInputs(modelView))
          .apply(Combine.perKey(new CombineFunction()));

      // Tags for CoGroupByKey.
      final TupleTag<List<Double>> gradientTag = new TupleTag<>();
      final TupleTag<List<Double>> modelTag = new TupleTag<>();
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

  /**
   * Main function for the MLR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final long start = System.currentTimeMillis();
    LOG.info(Arrays.toString(args));
    final String inputFilePath = args[0];
    final Integer numFeatures = Integer.parseInt(args[1]);
    final Integer numClasses = Integer.parseInt(args[2]);
    final Integer numItr = Integer.parseInt(args[3]);

    final List<Integer> initialModelKeys = new ArrayList<>(numClasses);
    for (Integer i = 0; i < numClasses; i++) {
      initialModelKeys.add(i);
    }

    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("MLR");
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);

    final Pipeline p = Pipeline.create(options);

    // Initialization of the model for Logistic Regression.
    PCollection<KV<Integer, List<Double>>> model = p
        .apply(Create.of(initialModelKeys))
        .apply(ParDo.of(new DoFn<Integer, KV<Integer, List<Double>>>() {
          @ProcessElement
          public void processElement(final ProcessContext c) throws Exception {
            if (c.element() == numClasses - 1) {
              final List<Double> model = new ArrayList<>(1);
              model.add(0.0);
              c.output(KV.of(c.element(), model));
            } else {
              final List<Double> model = new ArrayList<>(numFeatures);
              for (Integer i = 0; i < numFeatures; i++) {
                model.add(i, 0.0);
              }
              c.output(KV.of(c.element(), model));
            }
          }
        }));

    // Read input data
    final PCollection<String> readInput = GenericSourceSink.read(p, inputFilePath);

    // Multiple iterations for convergence.
    for (Integer i = 1; i <= numItr; i++) {
      // NOTE: a single composite transform for the iteration.
      model = model.apply(new UpdateModel(numFeatures, numClasses, i, readInput));
    }

    p.run();
    LOG.info("JCT " + (System.currentTimeMillis() - start));
  }
}

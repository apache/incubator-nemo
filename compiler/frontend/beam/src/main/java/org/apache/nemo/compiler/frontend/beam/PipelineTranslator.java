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
package org.apache.nemo.compiler.frontend.beam;

import com.google.common.collect.Iterables;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.BeamBoundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.transform.*;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A collection of translators for the Beam PTransforms.
 */
final class PipelineTranslator {
  public static final PipelineTranslator INSTANCE = new PipelineTranslator();
  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslator.class.getName());

  private final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator = new HashMap<>();
  private final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator = new HashMap<>();

  /**
   * Creates the translator, while building a map between {@link PTransform}s and the corresponding translators.
   */
  private PipelineTranslator() {
    for (final Method translator : getClass().getDeclaredMethods()) {
      final PrimitiveTransformTranslator primitive = translator.getAnnotation(PrimitiveTransformTranslator.class);
      final CompositeTransformTranslator composite = translator.getAnnotation(CompositeTransformTranslator.class);
      if (primitive != null) {
        for (final Class<? extends PTransform> transform : primitive.value()) {
          if (primitiveTransformToTranslator.containsKey(transform)) {
            throw new RuntimeException(String.format("Translator for primitive transform %s is"
              + "already registered: %s", transform, primitiveTransformToTranslator.get(transform)));
          }
          primitiveTransformToTranslator.put(transform, translator);
        }
      }
      if (composite != null) {
        for (final Class<? extends PTransform> transform : composite.value()) {
          if (compositeTransformToTranslator.containsKey(transform)) {
            throw new RuntimeException(String.format("Translator for composite transform %s is"
              + "already registered: %s", transform, compositeTransformToTranslator.get(transform)));
          }
          compositeTransformToTranslator.put(transform, translator);
        }
      }
    }
  }

  void translatePrimitive(final PipelineTranslationContext context,
                          final TransformHierarchy.Node primitive){
    final PTransform<?, ?> transform = primitive.getTransform();
    Class<?> clazz = transform.getClass();
    final Method translator = primitiveTransformToTranslator.get(clazz);
    if (translator == null) {
      throw new UnsupportedOperationException(
        String.format("Primitive transform %s is not supported", transform.getClass().getCanonicalName()));
    } else {
      try {
        translator.setAccessible(true);
        translator.invoke(null, context, primitive, transform);
      } catch (final IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (final InvocationTargetException | RuntimeException e) {
        throw new RuntimeException(String.format(
          "Translator %s have failed to translate %s", translator, transform), e);
      }
    }
  }

  /**
   * @param composite transform.
   * @return true if this composite has been translated in its entirety.
   */
  boolean translateComposite(final PipelineTranslationContext context,
                             final TransformHierarchy.Node composite){
    final PTransform<?, ?> transform = composite.getTransform();
    Class<?> clazz = transform.getClass();

    final Method translator = compositeTransformToTranslator.get(clazz);
    if (translator == null) {
      return false; // Failed to translate.
    } else {
      try {
        translator.setAccessible(true);
        translator.invoke(null, context, composite, transform);
        return true; // Translation succeeded!
      } catch (final IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (final InvocationTargetException | RuntimeException e) {
        throw new RuntimeException(String.format(
          "Translator %s have failed to translate %s", translator, transform), e);
      }
    }
  }

  /**
   * Annotates translator for PrimitiveTransform.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface PrimitiveTransformTranslator {
    Class<? extends PTransform>[] value();
  }

  /**
   * Annotates translator for CompositeTransform.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface CompositeTransformTranslator {
    Class<? extends PTransform>[] value();
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////// PRIMITIVE TRANSFORMS

  @PrimitiveTransformTranslator(Read.Unbounded.class)
  private static void unboundedReadTranslator(final PipelineTranslationContext ctx,
                                              final TransformHierarchy.Node transformVertex,
                                              final Read.Unbounded<?> transform) {
    final IRVertex vertex = new BeamUnboundedSourceVertex<>(transform.getSource());
    ctx.addVertex(vertex);
    transformVertex.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(Read.Bounded.class)
  private static void boundedReadTranslator(final PipelineTranslationContext ctx,
                                            final TransformHierarchy.Node transformVertex,
                                            final Read.Bounded<?> transform) {
    final IRVertex vertex = new BeamBoundedSourceVertex<>(transform.getSource());
    ctx.addVertex(vertex);
    transformVertex.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(ParDo.SingleOutput.class)
  private static void parDoSingleOutputTranslator(final PipelineTranslationContext ctx,
                                                  final TransformHierarchy.Node transformVertex,
                                                  final ParDo.SingleOutput<?, ?> transform) {
    final DoFnTransform doFnTransform = createDoFnTransform(ctx, transformVertex);
    final IRVertex vertex = new OperatorVertex(doFnTransform);

    ctx.addVertex(vertex);
    transformVertex.getInputs().values().stream()
      .filter(input -> !transform.getAdditionalInputs().values().contains(input))
      .forEach(input -> ctx.addEdgeTo(vertex, input));
    transform.getSideInputs().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(ParDo.MultiOutput.class)
  private static void parDoMultiOutputTranslator(final PipelineTranslationContext ctx,
                                                 final TransformHierarchy.Node transformVertex,
                                                 final ParDo.MultiOutput<?, ?> transform) {
    final DoFnTransform doFnTransform = createDoFnTransform(ctx, transformVertex);
    final IRVertex vertex = new OperatorVertex(doFnTransform);
    ctx.addVertex(vertex);
    transformVertex.getInputs().values().stream()
      .filter(input -> !transform.getAdditionalInputs().values().contains(input))
      .forEach(input -> ctx.addEdgeTo(vertex, input));
    transform.getSideInputs().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getOutputs().entrySet().stream()
      .filter(pValueWithTupleTag -> pValueWithTupleTag.getKey().equals(transform.getMainOutputTag()))
      .forEach(pValueWithTupleTag -> ctx.registerMainOutputFrom(vertex, pValueWithTupleTag.getValue()));
    transformVertex.getOutputs().entrySet().stream()
      .filter(pValueWithTupleTag -> !pValueWithTupleTag.getKey().equals(transform.getMainOutputTag()))
      .forEach(pValueWithTupleTag -> ctx.registerAdditionalOutputFrom(vertex, pValueWithTupleTag.getValue(),
        pValueWithTupleTag.getKey()));
  }

  @PrimitiveTransformTranslator(GroupByKey.class)
  private static void groupByKeyTranslator(final PipelineTranslationContext ctx,
                                           final TransformHierarchy.Node transformVertex,
                                           final GroupByKey<?, ?> transform) {
    final IRVertex vertex = new OperatorVertex(createGBKTransform(ctx, transformVertex));
    ctx.addVertex(vertex);
    transformVertex.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator({Window.class, Window.Assign.class})
  private static void windowTranslator(final PipelineTranslationContext ctx,
                                       final TransformHierarchy.Node transformVertex,
                                       final PTransform<?, ?> transform) {
    final WindowFn windowFn;
    if (transform instanceof Window) {
      windowFn = ((Window) transform).getWindowFn();
    } else if (transform instanceof Window.Assign) {
      windowFn = ((Window.Assign) transform).getWindowFn();
    } else {
      throw new UnsupportedOperationException(String.format("%s is not supported", transform));
    }
    final IRVertex vertex = new OperatorVertex(new WindowFnTransform(windowFn));
    ctx.addVertex(vertex);
    transformVertex.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(View.CreatePCollectionView.class)
  private static void createPCollectionViewTranslator(final PipelineTranslationContext ctx,
                                                      final TransformHierarchy.Node transformVertex,
                                                      final View.CreatePCollectionView<?, ?> transform) {
    final IRVertex vertex = new OperatorVertex(new CreateViewTransform(transform.getView().getViewFn()));
    ctx.addVertex(vertex);
    transformVertex.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    ctx.registerMainOutputFrom(vertex, transform.getView());
    transformVertex.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(Flatten.PCollections.class)
  private static void flattenTranslator(final PipelineTranslationContext ctx,
                                        final TransformHierarchy.Node transformVertex,
                                        final Flatten.PCollections<?> transform) {
    final IRVertex vertex = new OperatorVertex(new FlattenTransform());
    ctx.addVertex(vertex);
    transformVertex.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////// COMPOSITE TRANSFORMS

  /**
   * {@link Combine.PerKey} = {@link GroupByKey} + {@link Combine.GroupedValues}
   * ({@link Combine.Globally} internally uses {@link Combine.PerKey} which will also be optimized by this translator)
   *
   * Partial aggregation optimizations (e.g., combiner, aggregation tree) will be applied here.
   * In {@link Combine.CombineFn}, there are InputT, AccumT, OutputT
   * Partial aggregations will perform transformations of AccumT -> AccumT
   *
   * @param ctx provides translation context
   * @param transformVertex the given CompositeTransform to translate
   * @param transform transform which can be obtained from {@code transformVertex}
   */
  @CompositeTransformTranslator(Combine.PerKey.class)
  private static void combinePerKeyTranslator(final PipelineTranslationContext ctx,
                                              final TransformHierarchy.Node transformVertex,
                                              final PTransform<?, ?> transform) {
    /*
    // TODO #XXX: Combiner optimization for streaming
    if (!isBatch(transformVertex)) {
      transformVertex.getDAG().topologicalDo(ctx::translate);
      return;
    }

    LOG.info("---");
    transformVertex.getDAG().topologicalDo(v -> LOG.info(v.toString()));


    final Combine.PerKey perKey = (Combine.PerKey) transform;
    final CombineFnBase.GlobalCombineFn combineFn = perKey.getFn();


    final Coder accumulatorCoder;
    try {
      accumulatorCoder = combineFn.getAccumulatorCoder(ctx.getPipeline().getCoderRegistry(),
        inputCoder.getValueCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }

    ctx.addEdgeTo(groupByKeyIRVertex, outputFromGbk));


    // input of Combine -O2O-> partialCombine -shuffle/accumcoder-> finalCombine -?> next
    final IRVertex partialCombine = new OperatorVertex(new CombineFnPartialTransform<>(combineFn));
    ctx.addVertex(partialCombine);
    ctx.addEdgeTo();
    // The KV coder

    combineFn.createAccumulator()

    final IRVertex finalCombine = new OperatorVertex(new CombineFnFinalTransform<>(combineFn));
    ctx.addVertex(finalCombine);
    ctx.addEdgeTo();
    KvCoder.of(x, accumulatorCoder);





    final PipelineTranslationContext oneToOneEdgeContext = new PipelineTranslationContext(ctx,
      OneToOneCommunicationPatternSelector.INSTANCE);
    transformVertex.getDAG().topologicalDo(oneToOneEdgeContext::translate);

    // Attempt to translate the CompositeTransform again.
    // Add GroupByKey, which is the first transform in the given CompositeTransform.
    // Make sure it consumes the output from the last vertex in OneToOneEdge-translated hierarchy.
    final IRVertex groupByKeyIRVertex = new OperatorVertex(createGBKTransform(ctx, transformVertex));
    ctx.addVertex(groupByKeyIRVertex);
    afterGbk.getOutputs().values()
      .forEach(outputFromGbk -> ctx.addEdgeTo(groupByKeyIRVertex, outputFromGbk));
    gbk.getOutputs().values()
      .forEach(outputFromGroupByKey -> ctx.registerMainOutputFrom(groupByKeyIRVertex, outputFromGroupByKey));

    // Translate the remaining vertices.
    topologicalOrdering.stream().skip(1).forEach(ctx::translate);
    */
  }

  /**
   * @param ctx provides translation context
   * @param transformVertex the given CompositeTransform to translate
   * @param transform transform which can be obtained from {@code transformVertex}
   */
  @CompositeTransformTranslator(LoopCompositeTransform.class)
  private static void loopTranslator(final PipelineTranslationContext ctx,
                                     final TransformHierarchy.Node transformVertex,
                                     final LoopCompositeTransform<?, ?> transform) {
    // Do nothing here, as the context handles the loop vertex stack.
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////// HELPER METHODS

  private static DoFnTransform createDoFnTransform(final PipelineTranslationContext ctx,
                                                   final TransformHierarchy.Node transformVertex) {
    try {
      final AppliedPTransform pTransform = transformVertex.toAppliedPTransform(ctx.getPipeline());
      final DoFn doFn = ParDoTranslation.getDoFn(pTransform);
      final TupleTag mainOutputTag = ParDoTranslation.getMainOutputTag(pTransform);
      final List<PCollectionView<?>> sideInputs = ParDoTranslation.getSideInputs(pTransform);
      final TupleTagList additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(pTransform);

      final PCollection<?> mainInput = (PCollection<?>)
        Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));

      return new DoFnTransform(
        doFn,
        mainInput.getCoder(),
        getOutputCoders(pTransform),
        mainOutputTag,
        additionalOutputTags.getAll(),
        mainInput.getWindowingStrategy(),
        sideInputs,
        ctx.getPipelineOptions());
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<TupleTag<?>, Coder<?>> getOutputCoders(final AppliedPTransform<?, ?, ?> ptransform) {
    return ptransform
      .getOutputs()
      .entrySet()
      .stream()
      .filter(e -> e.getValue() instanceof PCollection)
      .collect(Collectors.toMap(e -> e.getKey(), e -> ((PCollection) e.getValue()).getCoder()));
  }

  /**
   * Create a group by key transform.
   * It returns GroupByKeyAndWindowDoFnTransform if window function is not default.
   * @param ctx translation context
   * @param transformVertex transform vertex
   * @return group by key transform
   */
  private static Transform createGBKTransform(
    final PipelineTranslationContext ctx,
    final TransformHierarchy.Node transformVertex) {
    final AppliedPTransform pTransform = transformVertex.toAppliedPTransform(ctx.getPipeline());
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));
    final TupleTag mainOutputTag = new TupleTag<>();

    if (isBatch(transformVertex, ctx.getPipeline())) {
      return new GroupByKeyTransform();
    } else {
      return new GroupByKeyAndWindowDoFnTransform(
        getOutputCoders(pTransform),
        mainOutputTag,
        Collections.emptyList(),  /*  GBK does not have additional outputs */
        mainInput.getWindowingStrategy(),
        Collections.emptyList(), /*  GBK does not have additional side inputs */
        ctx.getPipelineOptions(),
        SystemReduceFn.buffering(mainInput.getCoder()));
    }
  }

  private static boolean isBatch(final TransformHierarchy.Node transformVertex, final Pipeline pipeline) {
    final AppliedPTransform pTransform = transformVertex.toAppliedPTransform(pipeline);
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));
    return mainInput.getWindowingStrategy().getWindowFn() instanceof GlobalWindows;
  }
}

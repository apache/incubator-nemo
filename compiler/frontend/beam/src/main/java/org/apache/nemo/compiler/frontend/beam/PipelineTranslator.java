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
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.BeamBoundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.transform.*;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * A collection of translators for the Beam PTransforms.
 */
final class PipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslator.class.getName());

  private static final PipelineTranslator INSTANCE = new PipelineTranslator();

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

  void translatePrimitive(final TransformHierarchy.Node primitive){
    final PTransform<?, ?> transform = primitive.getTransform();
    Class<?> clazz = transform.getClass();
    final Method translator = primitiveTransformToTranslator.get(clazz);
    if (translator == null) {
      throw new UnsupportedOperationException(
        String.format("Primitive transform %s is not supported", transform.getClass().getCanonicalName()));
    } else {
      try {
        translator.setAccessible(true);
        translator.invoke(null, this, primitive, transform);
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
  Pipeline.PipelineVisitor.CompositeBehavior translateComposite(final TransformHierarchy.Node composite){
    final PTransform<?, ?> transform = composite.getTransform();
    Class<?> clazz = transform.getClass();

    final Method translator = compositeTransformToTranslator.get(clazz);
    if (translator == null) {
      return Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM; // No translator exists (enter!)
    } else {
      try {
        translator.setAccessible(true);
        translator.invoke(null, this, composite, transform);

        // Translator has successfully translated (Do not enter! (TODO: loop?)
        return Pipeline.PipelineVisitor.CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
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
   * Default translator for CompositeTransforms. Translates inner DAG without modifying {@link PipelineTranslationContext}.
   *
   * @param ctx provides translation context
   * @param transformVertex the given CompositeTransform to translate
   * @param transform transform which can be obtained from {@code transformVertex}
   */
  @CompositeTransformTranslator(PTransform.class)
  private static void topologicalTranslator(final PipelineTranslationContext ctx,
                                            final TransformHierarchy.Node transformVertex,
                                            final PTransform<?, ?> transform) {
    transformVertex.getDAG().topologicalDo(ctx::translate);
  }

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
  }

  /**
   * Pushes the loop vertex to the stack before translating the inner DAG, and pops it after the translation.
   *
   * @param ctx provides translation context
   * @param transformVertex the given CompositeTransform to translate
   * @param transform transform which can be obtained from {@code transformVertex}
   */
  @CompositeTransformTranslator(LoopCompositeTransform.class)
  private static void loopTranslator(final PipelineTranslationContext ctx,
                                     final TransformHierarchy.Node transformVertex,
                                     final LoopCompositeTransform<?, ?> transform) {
    final LoopVertex loopVertex = new LoopVertex(transformVertex.getFullName());
    ctx.builder.addVertex(loopVertex, ctx.loopVertexStack);
    ctx.builder.removeVertex(loopVertex);
    ctx.loopVertexStack.push(loopVertex);
    topologicalTranslator(ctx, transformVertex, transform);
    ctx.loopVertexStack.pop();
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////// HELPER METHODS

  private static DoFnTransform createDoFnTransform(final PipelineTranslationContext ctx,
                                                   final TransformHierarchy.Node transformVertex) {
    try {
      final AppliedPTransform pTransform = transformVertex.toAppliedPTransform(ctx.pipeline);
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
        ctx.pipelineOptions);
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
    final AppliedPTransform pTransform = transformVertex.toAppliedPTransform(ctx.pipeline);
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));
    final TupleTag mainOutputTag = new TupleTag<>();

    if (isBatch(transformVertex)) {
      return new GroupByKeyTransform();
    } else {
      return new GroupByKeyAndWindowDoFnTransform(
        getOutputCoders(pTransform),
        mainOutputTag,
        Collections.emptyList(),  /*  GBK does not have additional outputs */
        mainInput.getWindowingStrategy(),
        Collections.emptyList(), /*  GBK does not have additional side inputs */
        ctx.pipelineOptions,
        SystemReduceFn.buffering(mainInput.getCoder()));
    }
  }

  private static boolean isBatch(final TransformHierarchy.Node transformVertex) {
    final AppliedPTransform pTransform = transformVertex.toAppliedPTransform(ctx.pipeline);
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));
    return mainInput.getWindowingStrategy().getWindowFn() instanceof GlobalWindows;
  }



  private DAG<IRVertex, IREdge> translateToIRDAG(final TransformHierarchy.Node vertex,
                                                 final Pipeline pipeline,
                                                 final PipelineOptions pipelineOptions) {
    final PipelineTranslationContext ctx = new PipelineTranslationContext(vertex, pipeline, primitiveTransformToTranslator,
      compositeTransformToTranslator, DefaultCommunicationPatternSelector.INSTANCE, pipelineOptions);
    ctx.translate(vertex);
    return ctx.builder.build();
  }


  private static Coder<?> getCoder(final PValue input, final TransformHierarchy.Node pipeline) {
    final Coder<?> coder;
    if (input instanceof PCollection) {
      coder = ((PCollection) input).getCoder();
    } else if (input instanceof PCollectionView) {
      coder = getCoderForView((PCollectionView) input, pipeline);
    } else {
      throw new RuntimeException(String.format("Coder for PValue %s cannot be determined", input));
    }
    return coder;
  }

  /**
   * Get appropriate coder for {@link PCollectionView}.
   *
   * @param view {@link PCollectionView} from the corresponding {@link View.CreatePCollectionView} transform
   * @return appropriate {@link Coder} for {@link PCollectionView}
   */
  private static Coder<?> getCoderForView(final PCollectionView view, final TransformHierarchy.Node pipeline) {
    final TransformHierarchy.Node src = pipeline.getPrimitiveProducerOf(view);
    final Coder<?> baseCoder = src.getOutputs().values().stream()
      .filter(v -> v instanceof PCollection)
      .map(v -> (PCollection) v)
      .findFirst()
      .orElseThrow(() -> new RuntimeException(String.format("No incoming PCollection to %s", src)))
      .getCoder();
    final KvCoder<?, ?> inputKVCoder = (KvCoder) baseCoder;
    final ViewFn viewFn = view.getViewFn();
    if (viewFn instanceof PCollectionViews.IterableViewFn) {
      return IterableCoder.of(inputKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.ListViewFn) {
      return ListCoder.of(inputKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.MapViewFn) {
      return MapCoder.of(inputKVCoder.getKeyCoder(), inputKVCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.MultimapViewFn) {
      return MapCoder.of(inputKVCoder.getKeyCoder(), IterableCoder.of(inputKVCoder.getValueCoder()));
    } else if (viewFn instanceof PCollectionViews.SingletonViewFn) {
      return baseCoder;
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported viewFn %s", viewFn.getClass()));
    }
  }

  /**
   * Default implementation for {@link CommunicationPatternProperty.Value} selector.
   */
  private static final class DefaultCommunicationPatternSelector
    implements BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> {

    private static final DefaultCommunicationPatternSelector INSTANCE = new DefaultCommunicationPatternSelector();

    @Override
    public CommunicationPatternProperty.Value apply(final IRVertex src, final IRVertex dst) {
      final Class<?> constructUnionTableFn;
      try {
        constructUnionTableFn = Class.forName("org.apache.beam.sdk.transforms.join.CoGroupByKey$ConstructUnionTableFn");
      } catch (final ClassNotFoundException e) {
        throw new RuntimeException(e);
      }

      final Transform srcTransform = src instanceof OperatorVertex ? ((OperatorVertex) src).getTransform() : null;
      final Transform dstTransform = dst instanceof OperatorVertex ? ((OperatorVertex) dst).getTransform() : null;
      final DoFn srcDoFn = srcTransform instanceof DoFnTransform ? ((DoFnTransform) srcTransform).getDoFn() : null;

      if (srcDoFn != null && srcDoFn.getClass().equals(constructUnionTableFn)) {
        return CommunicationPatternProperty.Value.Shuffle;
      }
      if (srcTransform instanceof FlattenTransform) {
        return CommunicationPatternProperty.Value.OneToOne;
      }
      if (dstTransform instanceof GroupByKeyAndWindowDoFnTransform
        || dstTransform instanceof GroupByKeyTransform) {
        return CommunicationPatternProperty.Value.Shuffle;
      }
      if (dstTransform instanceof CreateViewTransform) {
        return CommunicationPatternProperty.Value.BroadCast;
      }
      return CommunicationPatternProperty.Value.OneToOne;
    }
  }

  /**
   * A {@link CommunicationPatternProperty.Value} selector which always emits OneToOne.
   */
  private static final class OneToOneCommunicationPatternSelector
    implements BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> {
    private static final OneToOneCommunicationPatternSelector INSTANCE = new OneToOneCommunicationPatternSelector();

    @Override
    public CommunicationPatternProperty.Value apply(final IRVertex src, final IRVertex dst) {
      return CommunicationPatternProperty.Value.OneToOne;
    }
  }
}

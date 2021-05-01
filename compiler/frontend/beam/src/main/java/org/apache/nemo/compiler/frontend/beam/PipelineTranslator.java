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
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.compiler.frontend.beam.source.BeamBoundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.transform.CreateViewTransform;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.coder.SideInputCoder;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

  /**
   * @param context provides translation context.
   * @param primitive primitive node.
   */
  void translatePrimitive(final PipelineTranslationContext context,
                          final TransformHierarchy.Node primitive) {
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
   * @param context context.
   * @param composite transform.
   * @return behavior controls whether or not child transforms are visited.
   */
  Pipeline.PipelineVisitor.CompositeBehavior translateComposite(final PipelineTranslationContext context,
                                                                final TransformHierarchy.Node composite) {
    final PTransform<?, ?> transform = composite.getTransform();
    if (transform == null) {
      // root beam node
      return Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
    }

    Class<?> clazz = transform.getClass();
    final Method translator = compositeTransformToTranslator.get(clazz);
    if (translator == null) {
      return Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
    } else {
      try {
        translator.setAccessible(true);
        return (Pipeline.PipelineVisitor.CompositeBehavior) translator.invoke(null, context, composite, transform);
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
    /**
     * @return primitive transform.
     */
    Class<? extends PTransform>[] value();
  }

  /**
   * Annotates translator for CompositeTransform.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface CompositeTransformTranslator {
    /**
     * @return composite transform.
     */
    Class<? extends PTransform>[] value();
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////// PRIMITIVE TRANSFORMS

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator(Read.Unbounded.class)
  private static void unboundedReadTranslator(final PipelineTranslationContext ctx,
                                              final TransformHierarchy.Node beamNode,
                                              final Read.Unbounded<?> transform) {
    final IRVertex vertex = new BeamUnboundedSourceVertex<>(transform.getSource(), DisplayData.from(transform));
    ctx.addVertex(vertex);
    beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, vertex, output));
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator(Read.Bounded.class)
  private static void boundedReadTranslator(final PipelineTranslationContext ctx,
                                            final TransformHierarchy.Node beamNode,
                                            final Read.Bounded<?> transform) {
    final IRVertex vertex = new BeamBoundedSourceVertex<>(transform.getSource(), DisplayData.from(transform));
    ctx.addVertex(vertex);
    beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, vertex, output));
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator(ParDo.SingleOutput.class)
  private static void parDoSingleOutputTranslator(final PipelineTranslationContext ctx,
                                                  final TransformHierarchy.Node beamNode,
                                                  final ParDo.SingleOutput<?, ?> transform) {
    final Map<Integer, PCollectionView<?>> sideInputMap = getSideInputMap(transform.getSideInputs());
    final AbstractDoFnTransform doFnTransform = createDoFnTransform(ctx, beamNode, sideInputMap);
    final IRVertex vertex = new OperatorVertex(doFnTransform);

    if (doFnTransform.isPushback()) {
      vertex.isPushback = true;
      LOG.info("Set isPushback true for {}", vertex.getId());
    }

    ctx.addVertex(vertex);
    beamNode.getInputs().values().stream()
      .filter(input -> !transform.getAdditionalInputs().values().contains(input))
      .forEach(input -> ctx.addEdgeTo(vertex, input));
    ctx.addSideInputEdges(vertex, sideInputMap);
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, vertex, output));
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator(ParDo.MultiOutput.class)
  private static void parDoMultiOutputTranslator(final PipelineTranslationContext ctx,
                                                 final TransformHierarchy.Node beamNode,
                                                 final ParDo.MultiOutput<?, ?> transform) {
    final Map<Integer, PCollectionView<?>> sideInputMap = getSideInputMap(transform.getSideInputs());
    final AbstractDoFnTransform doFnTransform = createDoFnTransform(ctx, beamNode, sideInputMap);
    final IRVertex vertex = new OperatorVertex(doFnTransform);
    ctx.addVertex(vertex);
    beamNode.getInputs().values().stream()
      .filter(input -> !transform.getAdditionalInputs().values().contains(input))
      .forEach(input -> ctx.addEdgeTo(vertex, input));
    ctx.addSideInputEdges(vertex, sideInputMap);
    beamNode.getOutputs().entrySet().stream()
      .filter(pValueWithTupleTag -> pValueWithTupleTag.getKey().equals(transform.getMainOutputTag()))
      .forEach(pValueWithTupleTag -> ctx.registerMainOutputFrom(beamNode, vertex, pValueWithTupleTag.getValue()));
    beamNode.getOutputs().entrySet().stream()
      .filter(pValueWithTupleTag -> !pValueWithTupleTag.getKey().equals(transform.getMainOutputTag()))
      .forEach(pValueWithTupleTag -> ctx.registerAdditionalOutputFrom(beamNode, vertex, pValueWithTupleTag.getValue(),
        pValueWithTupleTag.getKey()));
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator(GroupByKey.class)
  private static void groupByKeyTranslator(final PipelineTranslationContext ctx,
                                           final TransformHierarchy.Node beamNode,
                                           final GroupByKey<?, ?> transform) {
    final AppliedPTransform pTransform = beamNode.toAppliedPTransform(ctx.getPipeline());
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));


    final PCollection inputs = (PCollection) Iterables.getOnlyElement(
      TransformInputs.nonAdditionalInputs(beamNode.toAppliedPTransform(ctx.getPipeline())));
    final KvCoder inputCoder = (KvCoder) inputs.getCoder();
    final Coder outputCoder = getOutputCoders(pTransform).values().iterator().next();

    final TupleTag mainOutputTag = new TupleTag<>();

    // Stream data processing, using GBKTransform
    final CombineFnBase.GlobalCombineFn partialCombineFn = new GBKPartialCombineFn(
      ((KvCoder)outputCoder).getValueCoder());

    final CombineFnBase.GlobalCombineFn finalCombineFn = new GBKFinalCombineFn(
    ((KvCoder)outputCoder).getValueCoder());

    final SystemReduceFn partialSystemReduceFn =
      SystemReduceFn.combining(
        inputCoder.getKeyCoder(),
        AppliedCombineFn.withInputCoder(partialCombineFn,
          ctx.getPipeline().getCoderRegistry(), inputCoder,
          null,
          mainInput.getWindowingStrategy()));

   // final SystemReduceFn partialSystemReduceFn =
   //   SystemReduceFn.buffering(inputCoder);

    final SystemReduceFn finalSystemReduceFn =
      SystemReduceFn.combining(
        inputCoder.getKeyCoder(),
        AppliedCombineFn.withInputCoder(finalCombineFn,
          ctx.getPipeline().getCoderRegistry(),
          (KvCoder) outputCoder,
          null, mainInput.getWindowingStrategy()));

    // final SystemReduceFn finalSystemReduceFn =
    //  SystemReduceFn.buffering(outputCoder);

    final TupleTag<?> partialMainOutputTag = new TupleTag<>();
    final GBKFinalTransform partialCombineStreamTransform =
      new GBKFinalTransform(mainInput.getCoder(),
        inputCoder.getKeyCoder(),
        Collections.singletonMap(partialMainOutputTag, outputCoder),
        partialMainOutputTag,
        mainInput.getWindowingStrategy(),
        ctx.getPipelineOptions(),
        partialSystemReduceFn,
        DisplayData.from(beamNode.getTransform()),
        true);

    final GBKFinalTransform finalCombineStreamTransform =
      new GBKFinalTransform(outputCoder,
        inputCoder.getKeyCoder(),
        getOutputCoders(pTransform),
        Iterables.getOnlyElement(beamNode.getOutputs().keySet()),
        mainInput.getWindowingStrategy(),
        ctx.getPipelineOptions(),
        finalSystemReduceFn,
        DisplayData.from(beamNode.getTransform()),
        false);


    // Original vertex
    final IRVertex vertex = new OperatorVertex(
      createGBKTransform(pTransform, mainInput, ctx, beamNode,
        SystemReduceFn.buffering(mainInput.getCoder())));

    final OperatorVertex partialCombine = new OperatorVertex(partialCombineStreamTransform);
    partialCombine.isGBK = true;
    partialCombine.setPartialToFinalTransform(new PartialToFinalTransform((Combine.CombineFn) finalCombineFn));

    final OperatorVertex finalCombine = new OperatorVertex(finalCombineStreamTransform);
    finalCombine.isGBK = true;


    // (Step 1) Partial Combine
    // ctx.addVertex(partialCombine);
    // beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(partialCombine, input));

    // (Step 2) Final Combine
    // ctx.addVertex(finalCombine);
    // beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, finalCombine, output));

    // (Step 3) Adding an edge from partialCombine vertex to finalCombine vertex
    final IREdge edge = new IREdge(CommunicationPatternProperty.Value.OneToOne, partialCombine, finalCombine);
    final Coder intermediateCoder = outputCoder;
    ctx.setEdgeProperty(edge, intermediateCoder, mainInput.getWindowingStrategy().getWindowFn().windowCoder());
    // ctx.addEdge(edge, intermediateCoder, mainInput.getWindowingStrategy().getWindowFn().windowCoder());

    ((OperatorVertex) vertex).setPartialCombine(partialCombine);
    ((OperatorVertex) vertex).setFinalCombine(finalCombine);
    ((OperatorVertex) vertex).setPartialToFinalEdge(edge);
    ((OperatorVertex) vertex).setIsGlobalWindow(mainInput.getWindowingStrategy()
    .getWindowFn() instanceof GlobalWindows);

    vertex.isGBK = true;
    ctx.addVertex(vertex);
    beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, vertex, output));
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator({Window.class, Window.Assign.class})
  private static void windowTranslator(final PipelineTranslationContext ctx,
                                       final TransformHierarchy.Node beamNode,
                                       final PTransform<?, ?> transform) {
    final WindowFn windowFn;
    if (transform instanceof Window) {
      windowFn = ((Window) transform).getWindowFn();
    } else if (transform instanceof Window.Assign) {
      windowFn = ((Window.Assign) transform).getWindowFn();
    } else {
      throw new UnsupportedOperationException(String.format("%s is not supported", transform));
    }
    final IRVertex vertex = new OperatorVertex(
      new WindowFnTransform(windowFn, DisplayData.from(beamNode.getTransform())));
    ctx.addVertex(vertex);
    beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, vertex, output));
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator(View.CreatePCollectionView.class)
  private static void createPCollectionViewTranslator(final PipelineTranslationContext ctx,
                                                      final TransformHierarchy.Node beamNode,
                                                      final View.CreatePCollectionView<?, ?> transform) {
    final IRVertex vertex = new OperatorVertex(new CreateViewTransform(transform.getView().getViewFn()));
    vertex.isGBK = false;
    ctx.addVertex(vertex);
    beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    ctx.registerMainOutputFrom(beamNode, vertex, transform.getView());
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, vertex, output));
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   */
  @PrimitiveTransformTranslator(Flatten.PCollections.class)
  private static void flattenTranslator(final PipelineTranslationContext ctx,
                                        final TransformHierarchy.Node beamNode,
                                        final Flatten.PCollections<?> transform) {
    final IRVertex vertex = new OperatorVertex(new FlattenTransform());
    //vertex.isGBK = true;
    ctx.addVertex(vertex);
    beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, vertex, output));
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////// COMPOSITE TRANSFORMS

  /**
   * {@link Combine.PerKey} = {@link GroupByKey} + {@link Combine.GroupedValues}
   * ({@link Combine.Globally} internally uses {@link Combine.PerKey} which will also be optimized by this translator)
   * Here, we translate this composite transform as a whole, exploiting its accumulator semantics.
   *
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   * @return behavior controls whether or not child transforms are visited.
   */
  @CompositeTransformTranslator(Combine.PerKey.class)
  private static Pipeline.PipelineVisitor.CompositeBehavior combinePerKeyTranslator(
    final PipelineTranslationContext ctx,
    final TransformHierarchy.Node beamNode,
    final PTransform<?, ?> transform) {

    final Combine.PerKey perKey = (Combine.PerKey) transform;

    // Check if the partial combining optimization can be applied.
    // If not, simply use the default Combine implementation by entering into it.
    if (!(isMainInputBounded(beamNode, ctx.getPipeline()) && isGlobalWindow(beamNode, ctx.getPipeline()))) {

      final CombineFnBase.GlobalCombineFn combineFn = perKey.getFn();
      final PCollection inputs = (PCollection) Iterables.getOnlyElement(
        TransformInputs.nonAdditionalInputs(beamNode.toAppliedPTransform(ctx.getPipeline())));
      final KvCoder inputCoder = (KvCoder) inputs.getCoder();

      final Coder accumCoder;
      try {
        accumCoder = combineFn.getAccumulatorCoder(ctx.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
      } catch (CannotProvideCoderException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      LOG.info("Combine fn class name {}", combineFn.getClass());
      final AppliedPTransform pTransform = beamNode.toAppliedPTransform(ctx.getPipeline());

      final TupleTag mainOutputTag = new TupleTag<>();

      final PCollection<?> mainInput = (PCollection<?>)
        Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));

     final SystemReduceFn systemReduceFn =
        SystemReduceFn.combining(
          inputCoder.getKeyCoder(),
          AppliedCombineFn.withInputCoder(combineFn, ctx.getPipeline().getCoderRegistry(), inputCoder));

      final GBKFinalTransform gbkFinal =
        new GBKFinalTransform(
          mainInput.getCoder(),
          inputCoder.getKeyCoder(),
          getOutputCoders(pTransform),
          mainOutputTag,
          mainInput.getWindowingStrategy(),
          ctx.getPipelineOptions(),
          systemReduceFn,
          DisplayData.from(beamNode.getTransform()),
          false);

      // (Stage 2) final combine

      final IRVertex finalCombine = new OperatorVertex(gbkFinal);
      finalCombine.isGBK = true;

      ctx.addVertex(finalCombine);
      beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(finalCombine, input));
      beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, finalCombine, output));

      // return Pipeline.PipelineVisitor.CompositeBehavior.DO_NOT_ENTER_TRANSFORM;

      // Stream data processing, using GBKTransform
      final CombineFnBase.GlobalCombineFn partialCombineFn = new PartialCombineFn(
        (Combine.CombineFn) combineFn, accumCoder);
      final CombineFnBase.GlobalCombineFn finalCombineFn = new FinalCombineFn(
        (Combine.CombineFn) combineFn, accumCoder);
      final SystemReduceFn partialSystemReduceFn =
        SystemReduceFn.combining(
          inputCoder.getKeyCoder(),
          AppliedCombineFn.withInputCoder(partialCombineFn,
            ctx.getPipeline().getCoderRegistry(), inputCoder,
            null,
            mainInput.getWindowingStrategy()));
      final SystemReduceFn finalSystemReduceFn =
        SystemReduceFn.combining(
          inputCoder.getKeyCoder(),
          AppliedCombineFn.withInputCoder(finalCombineFn,
            ctx.getPipeline().getCoderRegistry(),
            KvCoder.of(inputCoder.getKeyCoder(),
              accumCoder),
            null, mainInput.getWindowingStrategy()));
      final TupleTag<?> partialMainOutputTag = new TupleTag<>();
      final GBKFinalTransform partialCombineStreamTransform =
        new GBKFinalTransform(mainInput.getCoder(),
          inputCoder.getKeyCoder(),
          Collections.singletonMap(partialMainOutputTag, KvCoder.of(inputCoder.getKeyCoder(), accumCoder)),
          partialMainOutputTag,
          mainInput.getWindowingStrategy(),
          ctx.getPipelineOptions(),
          partialSystemReduceFn,
          DisplayData.from(beamNode.getTransform()),
          true);

      final GBKFinalTransform finalCombineStreamTransform =
        new GBKFinalTransform(KvCoder.of(inputCoder.getKeyCoder(), accumCoder),
          inputCoder.getKeyCoder(),
          getOutputCoders(pTransform),
          Iterables.getOnlyElement(beamNode.getOutputs().keySet()),
          mainInput.getWindowingStrategy(),
          ctx.getPipelineOptions(),
          finalSystemReduceFn,
          DisplayData.from(beamNode.getTransform()),
          false);

      final OperatorVertex partialCombineVertex = new OperatorVertex(partialCombineStreamTransform);
      partialCombineVertex.isGBK = true;
      partialCombineVertex.setPartialToFinalTransform(
        new PartialToFinalTransform((Combine.CombineFn) finalCombineFn));

      final OperatorVertex finalCombineVertex = new OperatorVertex(finalCombineStreamTransform);
      finalCombine.isGBK = true;

      // (Step 1) Partial Combine
      // ctx.addVertex(partialCombineVertex);
      // beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(partialCombineVertex, input));

      // (Step 2) Final Combine
      // ctx.addVertex(finalCombineVertex);
      // beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, finalCombineVertex, output));

      // (Step 3) Adding an edge from partialCombine vertex to finalCombine vertex
      final IREdge edge = new IREdge(CommunicationPatternProperty.Value.OneToOne, partialCombineVertex,
        finalCombineVertex);
      final Coder intermediateCoder = KvCoder.of(inputCoder.getKeyCoder(), accumCoder);
      ctx.setEdgeProperty(edge, intermediateCoder, mainInput.getWindowingStrategy().getWindowFn().windowCoder());

      ((OperatorVertex) finalCombine).setPartialCombine(partialCombineVertex);
      ((OperatorVertex) finalCombine).setFinalCombine(finalCombineVertex);
      ((OperatorVertex) finalCombine).setPartialToFinalEdge(edge);

      // ctx.addEdge(edge, intermediateCoder, mainInput.getWindowingStrategy().getWindowFn().windowCoder());

      // This composite transform has been translated in its entirety.
      return Pipeline.PipelineVisitor.CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
    }

    if (!perKey.getSideInputs().isEmpty()) {
      // TODO #264: Partial Combining with Beam SideInputs
      return Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
    }

    // This Combine can be optimized as the following sequence of Nemo IRVertices.
    // Combine Input -> Combine(Partial Combine -> KV<InputT, AccumT> -> Final Combine) -> Combine Output
    final CombineFnBase.GlobalCombineFn combineFn = perKey.getFn();

    // (Step 1) To Partial Combine
    final IRVertex partialCombine = new OperatorVertex(new CombineFnPartialTransform<>(combineFn));
    ctx.addVertex(partialCombine);
    beamNode.getInputs().values().forEach(input -> ctx.addEdgeTo(partialCombine, input));

    // (Step 2) To Final Combine
    final PCollection input = (PCollection) Iterables.getOnlyElement(
      TransformInputs.nonAdditionalInputs(beamNode.toAppliedPTransform(ctx.getPipeline())));
    final KvCoder inputCoder = (KvCoder) input.getCoder();
    final Coder accumulatorCoder;
    try {
      accumulatorCoder =
        combineFn.getAccumulatorCoder(ctx.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException(e);
    }
    final IRVertex finalCombine = new OperatorVertex(new CombineFnFinalTransform<>(combineFn));
    ctx.addVertex(finalCombine);
    final IREdge edge = new IREdge(CommunicationPatternProperty.Value.Shuffle, partialCombine, finalCombine);
    ctx.addEdge(
      edge,
      KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder),
      input.getWindowingStrategy().getWindowFn().windowCoder());

    // (Step 3) To Combine Output
    beamNode.getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(beamNode, finalCombine, output));

    // This composite transform has been translated in its entirety.
    return Pipeline.PipelineVisitor.CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
  }

  /**
   * @param ctx provides translation context
   * @param beamNode the beam node to be translated
   * @param transform transform which can be obtained from {@code beamNode}
   * @return behavior controls whether or not child transforms are visited.
   */
  @CompositeTransformTranslator(LoopCompositeTransform.class)
  private static Pipeline.PipelineVisitor.CompositeBehavior loopTranslator(
    final PipelineTranslationContext ctx,
    final TransformHierarchy.Node beamNode,
    final LoopCompositeTransform<?, ?> transform) {
    // Do nothing here, as the context handles the loop vertex stack.
    // We just keep this method to signal that the loop vertex is acknowledged.
    return Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////// HELPER METHODS

  /**
   * @param viewList list of {@link PCollectionView}s.
   * @return map of side inputs.
   */
  private static Map<Integer, PCollectionView<?>> getSideInputMap(final List<PCollectionView<?>> viewList) {
    return IntStream.range(0, viewList.size()).boxed().collect(Collectors.toMap(Function.identity(), viewList::get));
  }

  /**
   * @param ctx provides translation context.
   * @param beamNode the beam node to be translated.
   * @param sideInputMap side inputs.
   * @return the created DoFnTransform.
   */
  private static AbstractDoFnTransform createDoFnTransform(final PipelineTranslationContext ctx,
                                                           final TransformHierarchy.Node beamNode,
                                                           final Map<Integer, PCollectionView<?>> sideInputMap) {
    try {
      final AppliedPTransform pTransform = beamNode.toAppliedPTransform(ctx.getPipeline());
      final DoFn doFn = ParDoTranslation.getDoFn(pTransform);
      final TupleTag mainOutputTag = ParDoTranslation.getMainOutputTag(pTransform);
      final TupleTagList additionalOutputTags = ParDoTranslation.getAdditionalOutputTags(pTransform);

      final PCollection<?> mainInput = (PCollection<?>)
        Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));

      final HasDisplayData displayData = (builder) -> {
        builder.add(DisplayData.item("name", beamNode.getFullName()));
      };

      if (sideInputMap.isEmpty()) {
        return new DoFnTransform(
          doFn,
          mainInput.getCoder(),
          getOutputCoders(pTransform),
          mainOutputTag,
          additionalOutputTags.getAll(),
          mainInput.getWindowingStrategy(),
          ctx.getPipelineOptions(),
          DisplayData.from(displayData));
      } else {

        // TODO remove: ad-hoc
        final PValue input =  beamNode.getInputs().values().iterator().next();
        final Coder elementCoder = ((PCollection) input).getCoder();
        final Coder windowCoder = ((PCollection) input).getWindowingStrategy().getWindowFn().windowCoder();

        // main input encoder decoder
        final WindowedValue.FullWindowedValueCoder coder = WindowedValue.getFullCoder(elementCoder, windowCoder);

        // side input encoder decoder
        final PCollectionView view = sideInputMap.values().iterator().next();
        final Coder viewCoder = PipelineTranslationContext.getCoderForView(view, ctx);
        final WindowedValue.FullWindowedValueCoder sideInputElementCoder =
          WindowedValue.getFullCoder(SideInputCoder.of(viewCoder), windowCoder);

        return new PushBackDoFnTransform(
          doFn,
          mainInput.getCoder(),
          getOutputCoders(pTransform),
          mainOutputTag,
          additionalOutputTags.getAll(),
          mainInput.getWindowingStrategy(),
          sideInputMap,
          ctx.getPipelineOptions(),
          DisplayData.from(displayData),
          coder,
          sideInputElementCoder);

      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param ptransform PTransform to get coders for its outputs
   * @return the output coders.
   */
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
   * @param beamNode the beam node to be translated
   * @return group by key transform
   */
  private static Transform createGBKTransform(
    final AppliedPTransform pTransform,
    final PCollection mainInput,
    final PipelineTranslationContext ctx,
    final TransformHierarchy.Node beamNode,
    final SystemReduceFn systemReduceFn) {

    final TupleTag mainOutputTag = new TupleTag<>();

    if (isGlobalWindow(beamNode, ctx.getPipeline())) {
      return new GroupByKeyTransform();
    } else {
      final PCollection inputs = (PCollection) Iterables.getOnlyElement(
        TransformInputs.nonAdditionalInputs(beamNode.toAppliedPTransform(ctx.getPipeline())));
      final KvCoder inputCoder = (KvCoder) inputs.getCoder();

      final GBKFinalTransform gbkFinal =
        new GBKFinalTransform(
          mainInput.getCoder(),
          inputCoder.getKeyCoder(),
          getOutputCoders(pTransform),
          mainOutputTag,
          mainInput.getWindowingStrategy(),
          ctx.getPipelineOptions(),
          systemReduceFn,
          DisplayData.from(beamNode.getTransform()),
          false);

      return gbkFinal;
    }
  }


  /**
   * @param beamNode the beam node to be translated.
   * @param pipeline pipeline.
   * @return true if the main input has global window.
   */
  private static boolean isGlobalWindow(final TransformHierarchy.Node beamNode, final Pipeline pipeline) {
    final AppliedPTransform pTransform = beamNode.toAppliedPTransform(pipeline);
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));
    return mainInput.getWindowingStrategy().getWindowFn() instanceof GlobalWindows;
  }

  /**
   * @param beamNode the beam node to be translated.
   * @param pipeline pipeline.
   * @return true if the main input bounded.
   */
  private static boolean isMainInputBounded(final TransformHierarchy.Node beamNode, final Pipeline pipeline) {
    final AppliedPTransform pTransform = beamNode.toAppliedPTransform(pipeline);
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));
    return mainInput.isBounded() == PCollection.IsBounded.BOUNDED;
  }
}

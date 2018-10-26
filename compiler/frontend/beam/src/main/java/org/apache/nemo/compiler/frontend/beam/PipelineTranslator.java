package org.apache.nemo.compiler.frontend.beam;

import com.google.common.collect.Iterables;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.PipelineVisitor.*;
import org.apache.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import org.apache.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Converts DAG of Beam root to Nemo IR DAG.
 * For a {@link PrimitiveTransformVertex}, it defines mapping to the corresponding {@link IRVertex}.
 * For a {@link CompositeTransformVertex}, it defines how to setup and clear {@link TranslationContext}
 * before start translating inner Beam transform hierarchy.
 */
public final class PipelineTranslator
  implements BiFunction<CompositeTransformVertex, PipelineOptions, DAG<IRVertex, IREdge>> {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslator.class.getName());

  private static final PipelineTranslator INSTANCE = new PipelineTranslator();

  private final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator = new HashMap<>();
  private final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator = new HashMap<>();

  // TODO #220: Move this variable to TranslationContext
  private static final AtomicReference<Pipeline> PIPELINE = new AtomicReference<>();

  /**
   * Static translator method.
   * @param pipeline the original root
   * @param root Top-level Beam transform hierarchy, usually given by {@link PipelineVisitor}
   * @param pipelineOptions {@link PipelineOptions}
   * @return Nemo IR DAG
   */
  public static DAG<IRVertex, IREdge> translate(final Pipeline pipeline,
                                                final CompositeTransformVertex root,
                                                final PipelineOptions pipelineOptions) {
    PIPELINE.set(pipeline);
    return INSTANCE.apply(root, pipelineOptions);
  }

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

  @PrimitiveTransformTranslator(Read.Unbounded.class)
  private static void unboundedReadTranslator(final TranslationContext ctx,
                                              final PrimitiveTransformVertex transformVertex,
                                              final Read.Unbounded<?> transform) {
    final IRVertex vertex = new BeamUnboundedSourceVertex<>(transform.getSource());
    ctx.addVertex(vertex);
    transformVertex.getNode().getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getNode().getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(Read.Bounded.class)
  private static void boundedReadTranslator(final TranslationContext ctx,
                                            final PrimitiveTransformVertex transformVertex,
                                            final Read.Bounded<?> transform) {
    final IRVertex vertex = new BeamBoundedSourceVertex<>(transform.getSource());
    ctx.addVertex(vertex);
    transformVertex.getNode().getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getNode().getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  private static DoFnTransform createDoFnTransform(final TranslationContext ctx,
                                                   final PrimitiveTransformVertex transformVertex) {
    try {
      final AppliedPTransform pTransform = transformVertex.getNode().toAppliedPTransform(PIPELINE.get());
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

  @PrimitiveTransformTranslator(ParDo.SingleOutput.class)
  private static void parDoSingleOutputTranslator(final TranslationContext ctx,
                                                  final PrimitiveTransformVertex transformVertex,
                                                  final ParDo.SingleOutput<?, ?> transform) {
    final DoFnTransform doFnTransform = createDoFnTransform(ctx, transformVertex);
    final IRVertex vertex = new OperatorVertex(doFnTransform);

    ctx.addVertex(vertex);
    transformVertex.getNode().getInputs().values().stream()
      .filter(input -> !transform.getAdditionalInputs().values().contains(input))
      .forEach(input -> ctx.addEdgeTo(vertex, input));
    transform.getSideInputs().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getNode().getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  private static Map<TupleTag<?>, Coder<?>> getOutputCoders(final AppliedPTransform<?, ?, ?> ptransform) {
    return ptransform
      .getOutputs()
      .entrySet()
      .stream()
      .filter(e -> e.getValue() instanceof PCollection)
      .collect(Collectors.toMap(e -> e.getKey(), e -> ((PCollection) e.getValue()).getCoder()));
  }

  @PrimitiveTransformTranslator(ParDo.MultiOutput.class)
  private static void parDoMultiOutputTranslator(final TranslationContext ctx,
                                                 final PrimitiveTransformVertex transformVertex,
                                                 final ParDo.MultiOutput<?, ?> transform) {
    final DoFnTransform doFnTransform = createDoFnTransform(ctx, transformVertex);
    final IRVertex vertex = new OperatorVertex(doFnTransform);
    ctx.addVertex(vertex);
    transformVertex.getNode().getInputs().values().stream()
      .filter(input -> !transform.getAdditionalInputs().values().contains(input))
      .forEach(input -> ctx.addEdgeTo(vertex, input));
    transform.getSideInputs().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getNode().getOutputs().entrySet().stream()
      .filter(pValueWithTupleTag -> pValueWithTupleTag.getKey().equals(transform.getMainOutputTag()))
      .forEach(pValueWithTupleTag -> ctx.registerMainOutputFrom(vertex, pValueWithTupleTag.getValue()));
    transformVertex.getNode().getOutputs().entrySet().stream()
      .filter(pValueWithTupleTag -> !pValueWithTupleTag.getKey().equals(transform.getMainOutputTag()))
      .forEach(pValueWithTupleTag -> ctx.registerAdditionalOutputFrom(vertex, pValueWithTupleTag.getValue(),
        pValueWithTupleTag.getKey()));
  }

  /**
   * Create a group by key transform.
   * It returns GroupByKeyAndWindowDoFnTransform if window function is not default.
   * @param ctx translation context
   * @param transformVertex transform vertex
   * @return group by key transform
   */
  private static Transform createGBKTransform(
    final TranslationContext ctx,
    final TransformVertex transformVertex) {
    final AppliedPTransform pTransform = transformVertex.getNode().toAppliedPTransform(PIPELINE.get());
    final PCollection<?> mainInput = (PCollection<?>)
      Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(pTransform));
    final TupleTag mainOutputTag = new TupleTag<>();

    if (mainInput.getWindowingStrategy() == WindowingStrategy.globalDefault()) {
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

  @PrimitiveTransformTranslator(GroupByKey.class)
  private static void groupByKeyTranslator(final TranslationContext ctx,
                                           final PrimitiveTransformVertex transformVertex,
                                           final GroupByKey<?, ?> transform) {
    final IRVertex vertex = new OperatorVertex(createGBKTransform(ctx, transformVertex));
    ctx.addVertex(vertex);
    transformVertex.getNode().getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getNode().getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator({Window.class, Window.Assign.class})
  private static void windowTranslator(final TranslationContext ctx,
                                       final PrimitiveTransformVertex transformVertex,
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
    transformVertex.getNode().getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getNode().getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(View.CreatePCollectionView.class)
  private static void createPCollectionViewTranslator(final TranslationContext ctx,
                                                      final PrimitiveTransformVertex transformVertex,
                                                      final View.CreatePCollectionView<?, ?> transform) {
    final IRVertex vertex = new OperatorVertex(new CreateViewTransform<>(transform.getView()));
    ctx.addVertex(vertex);
    transformVertex.getNode().getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    ctx.registerMainOutputFrom(vertex, transform.getView());
    transformVertex.getNode().getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  @PrimitiveTransformTranslator(Flatten.PCollections.class)
  private static void flattenTranslator(final TranslationContext ctx,
                                        final PrimitiveTransformVertex transformVertex,
                                        final Flatten.PCollections<?> transform) {
    final IRVertex vertex = new OperatorVertex(new FlattenTransform());
    ctx.addVertex(vertex);
    transformVertex.getNode().getInputs().values().forEach(input -> ctx.addEdgeTo(vertex, input));
    transformVertex.getNode().getOutputs().values().forEach(output -> ctx.registerMainOutputFrom(vertex, output));
  }

  /**
   * Default translator for CompositeTransforms. Translates inner DAG without modifying {@link TranslationContext}.
   *
   * @param ctx provides translation context
   * @param transformVertex the given CompositeTransform to translate
   * @param transform transform which can be obtained from {@code transformVertex}
   */
  @CompositeTransformTranslator(PTransform.class)
  private static void topologicalTranslator(final TranslationContext ctx,
                                            final CompositeTransformVertex transformVertex,
                                            final PTransform<?, ?> transform) {
    transformVertex.getDAG().topologicalDo(ctx::translate);
  }

  /**
   * Translator for Combine transform. Implements local combining before shuffling key-value pairs.
   *
   * @param ctx provides translation context
   * @param transformVertex the given CompositeTransform to translate
   * @param transform transform which can be obtained from {@code transformVertex}
   */
  @CompositeTransformTranslator({Combine.Globally.class, Combine.PerKey.class, Combine.GroupedValues.class})
  private static void combineTranslator(final TranslationContext ctx,
                                        final CompositeTransformVertex transformVertex,
                                        final PTransform<?, ?> transform) {
    // No optimization for BeamSQL that handles Beam 'Row's.
    final boolean handlesBeamRow = Stream
      .concat(transformVertex.getNode().getInputs().values().stream(),
        transformVertex.getNode().getOutputs().values().stream())
      .map(pValue -> (KvCoder) getCoder(pValue, ctx.root)) // Input and output of combine should be KV
      .map(kvCoder -> kvCoder.getValueCoder().getEncodedTypeDescriptor()) // We're interested in the 'Value' of KV
      .anyMatch(valueTypeDescriptor -> TypeDescriptor.of(Row.class).equals(valueTypeDescriptor));
    if (handlesBeamRow) {
      transformVertex.getDAG().topologicalDo(ctx::translate);
      return; // return early and give up optimization - TODO #209: Enable Local Combiner for BeamSQL
    }

    // Local combiner optimization
    final List<TransformVertex> topologicalOrdering = transformVertex.getDAG().getTopologicalSort();
    final TransformVertex groupByKeyBeamTransform = topologicalOrdering.get(0);
    final TransformVertex last = topologicalOrdering.get(topologicalOrdering.size() - 1);
    if (groupByKeyBeamTransform.getNode().getTransform() instanceof GroupByKey) {
      // Translate the given CompositeTransform under OneToOneEdge-enforced context.
      final TranslationContext oneToOneEdgeContext = new TranslationContext(ctx,
          OneToOneCommunicationPatternSelector.INSTANCE);
      transformVertex.getDAG().topologicalDo(oneToOneEdgeContext::translate);

      // Attempt to translate the CompositeTransform again.
      // Add GroupByKey, which is the first transform in the given CompositeTransform.
      // Make sure it consumes the output from the last vertex in OneToOneEdge-translated hierarchy.
      final IRVertex groupByKeyIRVertex = new OperatorVertex(createGBKTransform(ctx, transformVertex));
      ctx.addVertex(groupByKeyIRVertex);
      last.getNode().getOutputs().values().forEach(outputFromCombiner
          -> ctx.addEdgeTo(groupByKeyIRVertex, outputFromCombiner));
      groupByKeyBeamTransform.getNode().getOutputs().values()
          .forEach(outputFromGroupByKey -> ctx.registerMainOutputFrom(groupByKeyIRVertex, outputFromGroupByKey));

      // Translate the remaining vertices.
      topologicalOrdering.stream().skip(1).forEach(ctx::translate);
    } else {
      transformVertex.getDAG().topologicalDo(ctx::translate);
    }
  }

  /**
   * Pushes the loop vertex to the stack before translating the inner DAG, and pops it after the translation.
   *
   * @param ctx provides translation context
   * @param transformVertex the given CompositeTransform to translate
   * @param transform transform which can be obtained from {@code transformVertex}
   */
  @CompositeTransformTranslator(LoopCompositeTransform.class)
  private static void loopTranslator(final TranslationContext ctx,
                                     final CompositeTransformVertex transformVertex,
                                     final LoopCompositeTransform<?, ?> transform) {
    final LoopVertex loopVertex = new LoopVertex(transformVertex.getNode().getFullName());
    ctx.builder.addVertex(loopVertex, ctx.loopVertexStack);
    ctx.builder.removeVertex(loopVertex);
    ctx.loopVertexStack.push(loopVertex);
    topologicalTranslator(ctx, transformVertex, transform);
    ctx.loopVertexStack.pop();
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final CompositeTransformVertex pipeline,
                                     final PipelineOptions pipelineOptions) {
    final TranslationContext ctx = new TranslationContext(pipeline, primitiveTransformToTranslator,
        compositeTransformToTranslator, DefaultCommunicationPatternSelector.INSTANCE, pipelineOptions);
    ctx.translate(pipeline);
    return ctx.builder.build();
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

  private static Coder<?> getCoder(final PValue input, final CompositeTransformVertex pipeline) {
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
  private static Coder<?> getCoderForView(final PCollectionView view, final CompositeTransformVertex pipeline) {
    final PrimitiveTransformVertex src = pipeline.getPrimitiveProducerOf(view);
    final Coder<?> baseCoder = src.getNode().getInputs().values().stream()
      .filter(v -> v instanceof PCollection).map(v -> (PCollection) v).findFirst()
      .orElseThrow(() -> new RuntimeException(String.format("No incoming PCollection to %s", src)))
      .getCoder();
    final ViewFn viewFn = view.getViewFn();
    if (viewFn instanceof PCollectionViews.IterableViewFn) {
      return IterableCoder.of(baseCoder);
    } else if (viewFn instanceof PCollectionViews.ListViewFn) {
      return ListCoder.of(baseCoder);
    } else if (viewFn instanceof PCollectionViews.MapViewFn) {
      final KvCoder<?, ?> inputCoder = (KvCoder) baseCoder;
      return MapCoder.of(inputCoder.getKeyCoder(), inputCoder.getValueCoder());
    } else if (viewFn instanceof PCollectionViews.MultimapViewFn) {
      final KvCoder<?, ?> inputCoder = (KvCoder) baseCoder;
      return MapCoder.of(inputCoder.getKeyCoder(), IterableCoder.of(inputCoder.getValueCoder()));
    } else if (viewFn instanceof PCollectionViews.SingletonViewFn) {
      return baseCoder;
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported viewFn %s", viewFn.getClass()));
    }
  }

  /**
   * Translation context.
   */
  private static final class TranslationContext {
    private final CompositeTransformVertex root;
    private final PipelineOptions pipelineOptions;
    private final DAGBuilder<IRVertex, IREdge> builder;
    private final Map<PValue, IRVertex> pValueToProducer;
    private final Map<PValue, TupleTag<?>> pValueToTag;
    private final Stack<LoopVertex> loopVertexStack;
    private final BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> communicationPatternSelector;

    private final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator;
    private final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator;

    /**
     * @param root the root to translate
     * @param primitiveTransformToTranslator provides translators for PrimitiveTransform
     * @param compositeTransformToTranslator provides translators for CompositeTransform
     * @param selector provides {@link CommunicationPatternProperty.Value} for IR edges
     * @param pipelineOptions {@link PipelineOptions}
     */
    private TranslationContext(final CompositeTransformVertex root,
                               final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator,
                               final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator,
                               final BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> selector,
                               final PipelineOptions pipelineOptions) {
      this.root = root;
      this.builder = new DAGBuilder<>();
      this.pValueToProducer = new HashMap<>();
      this.pValueToTag = new HashMap<>();
      this.loopVertexStack = new Stack<>();
      this.primitiveTransformToTranslator = primitiveTransformToTranslator;
      this.compositeTransformToTranslator = compositeTransformToTranslator;
      this.communicationPatternSelector = selector;
      this.pipelineOptions = pipelineOptions;
    }

    /**
     * Copy constructor, except for setting different CommunicationPatternProperty selector.
     *
     * @param ctx the original {@link TranslationContext}
     * @param selector provides {@link CommunicationPatternProperty.Value} for IR edges
     */
    private TranslationContext(final TranslationContext ctx,
                               final BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> selector) {
      this.root = ctx.root;
      this.pipelineOptions = ctx.pipelineOptions;
      this.builder = ctx.builder;
      this.pValueToProducer = ctx.pValueToProducer;
      this.pValueToTag = ctx.pValueToTag;
      this.loopVertexStack = ctx.loopVertexStack;
      this.primitiveTransformToTranslator = ctx.primitiveTransformToTranslator;
      this.compositeTransformToTranslator = ctx.compositeTransformToTranslator;

      this.communicationPatternSelector = selector;
    }

    /**
     * Selects appropriate translator to translate the given hierarchy.
     *
     * @param transformVertex the Beam transform hierarchy to translate
     */
    private void translate(final TransformVertex transformVertex) {
      final boolean isComposite = transformVertex instanceof CompositeTransformVertex;
      final PTransform<?, ?> transform = transformVertex.getNode().getTransform();
      if (transform == null) {
        // root node
        topologicalTranslator(this, (CompositeTransformVertex) transformVertex, null);
        return;
      }

      Class<?> clazz = transform.getClass();
      while (true) {
        final Method translator = (isComposite ? compositeTransformToTranslator : primitiveTransformToTranslator)
            .get(clazz);
        if (translator == null) {
          if (clazz.getSuperclass() != null) {
            clazz = clazz.getSuperclass();
            continue;
          }
          throw new UnsupportedOperationException(String.format("%s transform %s is not supported",
              isComposite ? "Composite" : "Primitive", transform.getClass().getCanonicalName()));
        } else {
          try {
            translator.setAccessible(true);
            translator.invoke(null, this, transformVertex, transform);
            break;
          } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
          } catch (final InvocationTargetException | RuntimeException e) {
            throw new RuntimeException(String.format(
                "Translator %s have failed to translate %s", translator, transform), e);
          }
        }
      }
    }

    /**
     * Add IR vertex to the builder.
     *
     * @param vertex IR vertex to add
     */
    private void addVertex(final IRVertex vertex) {
      builder.addVertex(vertex, loopVertexStack);
    }

    /**
     * Add IR edge to the builder.
     *
     * @param dst the destination IR vertex.
     * @param input the {@link PValue} {@code dst} consumes
     */
    private void addEdgeTo(final IRVertex dst, final PValue input) {
      final IRVertex src = pValueToProducer.get(input);
      if (src == null) {
        try {
          throw new RuntimeException(String.format("Cannot find a vertex that emits pValue %s, "
              + "while PTransform %s is known to produce it.", input, root.getPrimitiveProducerOf(input)));
        } catch (final RuntimeException e) {
          throw new RuntimeException(String.format("Cannot find a vertex that emits pValue %s, "
              + "and the corresponding PTransform was not found", input));
        }
      }
      final CommunicationPatternProperty.Value communicationPattern = communicationPatternSelector.apply(src, dst);
      if (communicationPattern == null) {
        throw new RuntimeException(String.format("%s have failed to determine communication pattern "
            + "for an edge from %s to %s", communicationPatternSelector, src, dst));
      }
      final IREdge edge = new IREdge(communicationPattern, src, dst);
      final Coder coder;
      final Coder windowCoder;
      if (input instanceof PCollection) {
        coder = ((PCollection) input).getCoder();
        windowCoder = ((PCollection) input).getWindowingStrategy().getWindowFn().windowCoder();
      } else if (input instanceof PCollectionView) {
        coder = getCoderForView((PCollectionView) input, root);
        windowCoder = ((PCollectionView) input).getPCollection()
          .getWindowingStrategy().getWindowFn().windowCoder();
      } else {
        throw new RuntimeException(String.format("While adding an edge from %s, to %s, coder for PValue %s cannot "
          + "be determined", src, dst, input));
      }

      edge.setProperty(KeyExtractorProperty.of(new BeamKeyExtractor()));

      if (coder instanceof KvCoder) {
        Coder keyCoder = ((KvCoder) coder).getKeyCoder();
        edge.setProperty(KeyEncoderProperty.of(new BeamEncoderFactory(keyCoder)));
        edge.setProperty(KeyDecoderProperty.of(new BeamDecoderFactory(keyCoder)));
      }

      edge.setProperty(EncoderProperty.of(
        new BeamEncoderFactory<>(WindowedValue.getFullCoder(coder, windowCoder))));
      edge.setProperty(DecoderProperty.of(
        new BeamDecoderFactory<>(WindowedValue.getFullCoder(coder, windowCoder))));

      if (pValueToTag.containsKey(input)) {
        edge.setProperty(AdditionalOutputTagProperty.of(pValueToTag.get(input).getId()));
      }

      if (input instanceof PCollectionView) {
        edge.setProperty(BroadcastVariableIdProperty.of((PCollectionView) input));
      }

      builder.connectVertices(edge);
    }

    /**
     * Registers a {@link PValue} as a main output from the specified {@link IRVertex}.
     *
     * @param irVertex the IR vertex
     * @param output the {@link PValue} {@code irVertex} emits as main output
     */
    private void registerMainOutputFrom(final IRVertex irVertex, final PValue output) {
      pValueToProducer.put(output, irVertex);
    }

    /**
     * Registers a {@link PValue} as an additional output from the specified {@link IRVertex}.
     *
     * @param irVertex the IR vertex
     * @param output the {@link PValue} {@code irVertex} emits as additional output
     * @param tag the {@link TupleTag} associated with this additional output
     */
    private void registerAdditionalOutputFrom(final IRVertex irVertex, final PValue output, final TupleTag<?> tag) {
      pValueToTag.put(output, tag);
      pValueToProducer.put(output, irVertex);
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

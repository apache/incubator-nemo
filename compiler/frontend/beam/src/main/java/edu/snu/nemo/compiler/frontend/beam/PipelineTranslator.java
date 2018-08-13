/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.compiler.frontend.beam;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.compiler.frontend.beam.PipelineVisitor.*;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import edu.snu.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
import edu.snu.nemo.compiler.frontend.beam.source.BeamBoundedSourceVertex;
import edu.snu.nemo.compiler.frontend.beam.transform.*;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.*;

import java.lang.annotation.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Converts DAG of Beam pipeline to Nemo IR DAG.
 */
public final class PipelineTranslator implements Function<CompositeTransformVertex, DAG<IRVertex, IREdge>> {
  private final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator = new HashMap<>();
  private final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator = new HashMap<>();
  private final PipelineOptions pipelineOptions;

  public PipelineTranslator(final PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
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

  private static void topologicalTranslator(final TranslationContext ctx,
                                            final CompositeTransformVertex transformVertex,
                                            final PTransform transform) {
    transformVertex.getDAG().topologicalDo(vertex -> ctx.translate(vertex));
  }

  @PrimitiveTransformTranslator(Read.Bounded.class)
  private static void boundedReadTranslator(final TranslationContext ctx,
                                            final PrimitiveTransformVertex transformVertex,
                                            final Read.Bounded transform) {
    final IRVertex vertex = new BeamBoundedSourceVertex<>(transform.getSource());
    ctx.builder.addVertex(vertex);
    ctx.addEdgesTo(vertex, transformVertex.getNode().getInputs().values(), false);
    ctx.registerOutputsFrom(vertex, transformVertex.getNode().getOutputs().values());
  }

  @PrimitiveTransformTranslator(ParDo.MultiOutput.class)
  private static void parDoMultiOutputTranslator(final TranslationContext ctx,
                                                 final PrimitiveTransformVertex transformVertex,
                                                 final ParDo.MultiOutput transform) {
    final DoTransform doTransform = new DoTransform(transform.getFn(), ctx.pipelineOptions);
    final IRVertex vertex = new OperatorVertex(doTransform);
    transformVertex.getNode().getOutputs().entrySet().stream()
        .filter(pValueWithTupleTag -> !pValueWithTupleTag.getKey().equals(transform.getMainOutputTag()))
        .forEach(pValueWithTupleTag -> ctx.pValueToTag.put(pValueWithTupleTag.getValue(), pValueWithTupleTag.getKey()));
    ctx.builder.addVertex(vertex);
    ctx.addEdgesTo(vertex, transformVertex.getNode().getInputs().values(), false);
    ctx.addEdgesTo(vertex, transform.getSideInputs(), true);
    ctx.registerOutputsFrom(vertex, transformVertex.getNode().getOutputs().values());
  }

  @PrimitiveTransformTranslator(GroupByKey.class)
  private static void groupByKeyTranslator(final TranslationContext ctx,
                                           final PrimitiveTransformVertex transformVertex,
                                           final GroupByKey transform) {
    final IRVertex vertex = new OperatorVertex(new GroupByKeyTransform());
    ctx.builder.addVertex(vertex);
    ctx.addEdgesTo(vertex, transformVertex.getNode().getInputs().values(), false);
    ctx.registerOutputsFrom(vertex, transformVertex.getNode().getOutputs().values());
  }

  @PrimitiveTransformTranslator({Window.class, Window.Assign.class})
  private static void windowTranslator(final TranslationContext ctx,
                                       final PrimitiveTransformVertex transformVertex,
                                       final PTransform transform) {
    final WindowFn windowFn;
    if (transform instanceof Window) {
      windowFn = ((Window) transform).getWindowFn();
    } else if (transform instanceof Window.Assign) {
      windowFn = ((Window.Assign) transform).getWindowFn();
    } else {
      throw new UnsupportedOperationException(String.format("%s is not supported", transform));
    }
    final IRVertex vertex = new OperatorVertex(new WindowTransform(windowFn));
    ctx.builder.addVertex(vertex);
    ctx.addEdgesTo(vertex, transformVertex.getNode().getInputs().values(), false);
    ctx.registerOutputsFrom(vertex, transformVertex.getNode().getOutputs().values());
  }

  @PrimitiveTransformTranslator(View.CreatePCollectionView.class)
  private static void createPCollectionViewTranslator(final TranslationContext ctx,
                                                      final PrimitiveTransformVertex transformVertex,
                                                      final View.CreatePCollectionView transform) {
    final IRVertex vertex = new OperatorVertex(new CreateViewTransform(transform.getView()));
    ctx.builder.addVertex(vertex);
    ctx.addEdgesTo(vertex, transformVertex.getNode().getInputs().values(), false);
    ctx.registerOutputsFrom(vertex, Collections.singleton(transform.getView()));
    ctx.registerOutputsFrom(vertex, transformVertex.getNode().getOutputs().values());
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final CompositeTransformVertex pipeline) {
    final TranslationContext ctx = new TranslationContext(pipeline, primitiveTransformToTranslator,
        compositeTransformToTranslator, pipelineOptions);
    ctx.translate(pipeline);
    return ctx.builder.build();
  }

  /**
   * Primitive transform translator.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface PrimitiveTransformTranslator {
    Class<? extends PTransform>[] value();
  }

  /**
   * Composite transform translator.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface CompositeTransformTranslator {
    Class<? extends PTransform>[] value();
  }

  /**
   * Ctx.
   */
  private static final class TranslationContext {
    private final PipelineOptions pipelineOptions;
    private final CompositeTransformVertex pipeline;
    private final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    private final Map<PValue, IRVertex> pValueToProducer = new HashMap<>();
    private final Map<PValue, TupleTag<?>> pValueToTag = new HashMap<>();
    private final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator;
    private final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator;

    private TranslationContext(final CompositeTransformVertex pipeline,
                               final Map<Class<? extends PTransform>, Method> primitiveTransformToTranslator,
                               final Map<Class<? extends PTransform>, Method> compositeTransformToTranslator,
                               final PipelineOptions pipelineOptions) {
      this.pipeline = pipeline;
      this.primitiveTransformToTranslator = primitiveTransformToTranslator;
      this.compositeTransformToTranslator = compositeTransformToTranslator;
      this.pipelineOptions = pipelineOptions;
    }

    private void translate(final TransformVertex transformVertex) {
      final boolean isComposite = transformVertex instanceof CompositeTransformVertex;
      final PTransform transform = transformVertex.getNode().getTransform();
      if (transform == null) {
        // root node
        topologicalTranslator(this, (CompositeTransformVertex) transformVertex, null);
        return;
      }
      final Method translator = (isComposite ? compositeTransformToTranslator : primitiveTransformToTranslator)
          .get(transform.getClass());
      if (translator == null) {
        if (isComposite) {
          // default translator for CompositeTransform
          topologicalTranslator(this, (CompositeTransformVertex) transformVertex, transform);
        } else {
          throw new UnsupportedOperationException(String.format("Primitive transform %s is not supported",
              transform.getClass().getCanonicalName()));
        }
      } else {
        try {
          translator.setAccessible(true);
          translator.invoke(null, this, transformVertex, transform);
        } catch (final IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        } catch (final RuntimeException e) {
          throw new RuntimeException(String.format(
              "Translator %s have failed to translate %s", translator, transform), e);
        }
      }
    }

    private void addEdgesTo(final IRVertex dst,
                            final Collection<? extends PValue> inputs,
                            final boolean isSideInput,
                            final BiFunction<IRVertex, IRVertex, CommunicationPatternProperty.Value> cPatternFunc) {
      for (final PValue pValue : inputs) {
        final IRVertex src = pValueToProducer.get(pValue);
        final CommunicationPatternProperty.Value communicationPattern = cPatternFunc.apply(src, dst);
        if (communicationPattern == null) {
          throw new RuntimeException(String.format("%s have failed to determine communication pattern "
              + "for an edge from %s to %s", cPatternFunc, src, dst));
        }
        final IREdge edge = new IREdge(communicationPattern, src, dst, isSideInput);
        final Coder coder;
        if (pValue instanceof PCollection) {
          coder = ((PCollection) pValue).getCoder();
        } else if (pValue instanceof PCollectionView) {
          coder = getCoderForView((PCollectionView) pValue);
        } else {
          coder = null;
        }
        if (coder == null) {
          throw new RuntimeException(String.format("While adding an edge from %s, to %s, coder for PValue %s cannot "
              + "be determined", src, dst, pValue));
        }
        edge.setProperty(EncoderProperty.of(new BeamEncoderFactory(coder)));
        edge.setProperty(DecoderProperty.of(new BeamDecoderFactory(coder)));
        if (pValueToTag.containsKey(pValue)) {
          edge.setProperty(AdditionalOutputTagProperty.of(pValueToTag.get(pValue).getId()));
        }
        edge.setProperty(KeyExtractorProperty.of(new BeamKeyExtractor()));
        builder.connectVertices(edge);
      }

    }

    private void addEdgesTo(final IRVertex dst, final Collection<? extends PValue> inputs, final boolean isSideInput) {
      addEdgesTo(dst, inputs, isSideInput, DefaultCommunicationPatternSelector.INSTANCE);
    }

    private void registerOutputsFrom(final IRVertex irVertex, final Collection<PValue> outputs) {
      outputs.forEach(output -> pValueToProducer.put(output, irVertex));
    }

    /**
     * Get appropriate coder for {@link PCollectionView}.
     *
     * @param view {@link PCollectionView} from the corresponding {@link View.CreatePCollectionView} transform
     * @return appropriate {@link Coder} for {@link PCollectionView}
     */
    private Coder getCoderForView(final PCollectionView view) {
      final PrimitiveTransformVertex src = pipeline.getPrimitiveProducerOf(view);
      final Coder baseCoder = src.getNode().getInputs().values().stream()
          .filter(v -> v instanceof PCollection).map(v -> (PCollection) v).findFirst()
          .orElseThrow(() -> new RuntimeException(String.format("No incoming PCollection to %s", src)))
          .getCoder();
      final ViewFn viewFn = view.getViewFn();
      if (viewFn instanceof PCollectionViews.IterableViewFn) {
        return IterableCoder.of(baseCoder);
      } else if (viewFn instanceof PCollectionViews.ListViewFn) {
        return ListCoder.of(baseCoder);
      } else if (viewFn instanceof PCollectionViews.MapViewFn) {
        final KvCoder inputCoder = (KvCoder) baseCoder;
        return MapCoder.of(inputCoder.getKeyCoder(), inputCoder.getValueCoder());
      } else if (viewFn instanceof PCollectionViews.MultimapViewFn) {
        final KvCoder inputCoder = (KvCoder) baseCoder;
        return MapCoder.of(inputCoder.getKeyCoder(), IterableCoder.of(inputCoder.getValueCoder()));
      } else if (viewFn instanceof PCollectionViews.SingletonViewFn) {
        return baseCoder;
      } else {
        throw new UnsupportedOperationException(String.format("Unsupported viewFn %s", viewFn.getClass()));
      }
    }
  }

  /**
   * Selector.
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
      final DoFn srcDoFn = srcTransform instanceof DoTransform ? ((DoTransform) srcTransform).getDoFn() : null;

      if (srcDoFn != null && srcDoFn.getClass().equals(constructUnionTableFn)) {
        return CommunicationPatternProperty.Value.Shuffle;
      }
      if (srcTransform instanceof FlattenTransform) {
        return CommunicationPatternProperty.Value.OneToOne;
      }
      if (dstTransform instanceof GroupByKeyTransform) {
        return CommunicationPatternProperty.Value.Shuffle;
      }
      if (dstTransform instanceof CreateViewTransform) {
        return CommunicationPatternProperty.Value.BroadCast;
      }
      return CommunicationPatternProperty.Value.OneToOne;
    }
  }
}

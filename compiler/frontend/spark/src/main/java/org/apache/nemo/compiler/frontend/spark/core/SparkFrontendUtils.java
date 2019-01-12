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
package org.apache.nemo.compiler.frontend.spark.core;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.compiler.frontend.spark.SparkBroadcastVariables;
import org.apache.nemo.compiler.frontend.spark.SparkKeyExtractor;
import org.apache.nemo.compiler.frontend.spark.coder.SparkDecoderFactory;
import org.apache.nemo.compiler.frontend.spark.coder.SparkEncoderFactory;
import org.apache.nemo.compiler.frontend.spark.transform.CollectTransform;
import org.apache.nemo.compiler.frontend.spark.transform.GroupByKeyTransform;
import org.apache.nemo.compiler.frontend.spark.transform.ReduceByKeyTransform;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.*;
import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.TraversableOnce;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Utility class for RDDs.
 */
public final class SparkFrontendUtils {
  private static final KeyExtractorProperty SPARK_KEY_EXTRACTOR_PROP = KeyExtractorProperty.of(new SparkKeyExtractor());

  /**
   * Private constructor.
   */
  private SparkFrontendUtils() {
  }

  /**
   * Derive Spark serializer from a spark context.
   *
   * @param sparkContext spark context to derive the serializer from.
   * @return the serializer.
   */
  public static Serializer deriveSerializerFrom(final org.apache.spark.SparkContext sparkContext) {
    if (sparkContext.conf().get("spark.serializer", "")
        .equals("org.apache.spark.serializer.KryoSerializer")) {
      return new KryoSerializer(sparkContext.conf());
    } else {
      return new JavaSerializer(sparkContext.conf());
    }
  }

  /**
   * Collect data by running the DAG.
   *
   * @param dag             the DAG to execute.
   * @param loopVertexStack loop vertex stack.
   * @param lastVertex      last vertex added to the dag.
   * @param serializer      serializer for the edges.
   * @param <T>             type of the return data.
   * @return the data collected.
   */
  public static <T> List<T> collect(final DAG<IRVertex, IREdge> dag,
                                    final Stack<LoopVertex> loopVertexStack,
                                    final IRVertex lastVertex,
                                    final Serializer serializer) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex collectVertex = new OperatorVertex(new CollectTransform<>());
    builder.addVertex(collectVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, collectVertex),
        lastVertex, collectVertex);
    newEdge.setProperty(EncoderProperty.of(new SparkEncoderFactory(serializer)));
    newEdge.setProperty(DecoderProperty.of(new SparkDecoderFactory(serializer)));
    newEdge.setProperty(SPARK_KEY_EXTRACTOR_PROP);
    builder.connectVertices(newEdge);

    // launch DAG
    JobLauncher.launchDAG(builder.build(), SparkBroadcastVariables.getAll(), "");

    return (List<T>) JobLauncher.getCollectedData();
  }

  /**
   * Retrieve communication pattern of the edge.
   *
   * @param src source vertex.
   * @param dst destination vertex.
   * @return the communication pattern.
   */
  public static CommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
                                                                               final IRVertex dst) {
    if (dst instanceof OperatorVertex
        && (((OperatorVertex) dst).getTransform() instanceof ReduceByKeyTransform
        || ((OperatorVertex) dst).getTransform() instanceof GroupByKeyTransform)) {
      return CommunicationPatternProperty.Value.Shuffle;
    } else {
      return CommunicationPatternProperty.Value.OneToOne;
    }
  }

  /**
   * Converts a {@link Function1} to a corresponding {@link Function}.
   *
   * Here, we use the Spark 'JavaSerializer' to facilitate debugging in the future.
   * TODO #205: RDD Closure with Broadcast Variables Serialization Bug
   *
   * @param scalaFunction the scala function to convert.
   * @param <I>           the type of input.
   * @param <O>           the type of output.
   * @return the converted Java function.
   */
  public static <I, O> Function<I, O> toJavaFunction(final Function1<I, O> scalaFunction) {
    // This 'JavaSerializer' from Spark provides a human-readable NotSerializableException stack traces,
    // which can be useful when addressing this problem.
    // Other toJavaFunction can also use this serializer when debugging.
    final ClassTag<Function1<I, O>> classTag = ClassTag$.MODULE$.apply(scalaFunction.getClass());
    final byte[] serializedFunction = new JavaSerializer().newInstance().serialize(scalaFunction, classTag).array();

    return new Function<I, O>() {
      private Function1<I, O> deserializedFunction;

      @Override
      public O call(final I v1) throws Exception {
        if (deserializedFunction == null) {
          // TODO #205: RDD Closure with Broadcast Variables Serialization Bug
          final SerializerInstance js = new JavaSerializer().newInstance();
          deserializedFunction = js.deserialize(ByteBuffer.wrap(serializedFunction), classTag);
        }
        return deserializedFunction.apply(v1);
      }
    };
  }

  /**
   * Converts a {@link scala.Function2} to a corresponding {@link org.apache.spark.api.java.function.Function2}.
   *
   * @param scalaFunction the scala function to convert.
   * @param <I1>          the type of first input.
   * @param <I2>          the type of second input.
   * @param <O>           the type of output.
   * @return the converted Java function.
   */
  public static <I1, I2, O> Function2<I1, I2, O> toJavaFunction(final scala.Function2<I1, I2, O> scalaFunction) {
    return new Function2<I1, I2, O>() {
      @Override
      public O call(final I1 v1, final I2 v2) throws Exception {
        return scalaFunction.apply(v1, v2);
      }
    };
  }

  /**
   * Converts a {@link Function1} to a corresponding {@link FlatMapFunction}.
   *
   * @param scalaFunction the scala function to convert.
   * @param <I>           the type of input.
   * @param <O>           the type of output.
   * @return the converted Java function.
   */
  public static <I, O> FlatMapFunction<I, O> toJavaFlatMapFunction(
      final Function1<I, TraversableOnce<O>> scalaFunction) {
    return new FlatMapFunction<I, O>() {
      @Override
      public Iterator<O> call(final I i) throws Exception {
        return JavaConverters.asJavaIteratorConverter(scalaFunction.apply(i).toIterator()).asJava();
      }
    };
  }

  /**
   * Converts a {@link PairFunction} to a plain map {@link Function}.
   *
   * @param pairFunction the pair function to convert.
   * @param <T>          the type of original element.
   * @param <K>          the type of converted key.
   * @param <V>          the type of converted value.
   * @return the converted map function.
   */
  public static <T, K, V> Function<T, Tuple2<K, V>> pairFunctionToPlainFunction(
      final PairFunction<T, K, V> pairFunction) {
    return new Function<T, Tuple2<K, V>>() {
      @Override
      public Tuple2<K, V> call(final T elem) throws Exception {
        return pairFunction.call(elem);
      }
    };
  }
}

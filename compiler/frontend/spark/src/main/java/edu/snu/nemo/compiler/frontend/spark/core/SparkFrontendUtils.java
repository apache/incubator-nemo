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
package edu.snu.nemo.compiler.frontend.spark.core;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.EncoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor;
import edu.snu.nemo.compiler.frontend.spark.coder.SparkDecoderFactory;
import edu.snu.nemo.compiler.frontend.spark.coder.SparkEncoderFactory;
import edu.snu.nemo.compiler.frontend.spark.transform.CollectTransform;
import edu.snu.nemo.compiler.frontend.spark.transform.GroupByKeyTransform;
import edu.snu.nemo.compiler.frontend.spark.transform.ReduceByKeyTransform;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.TraversableOnce;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

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

    // save result in a temporary file
    // TODO #16: Implement collection of data from executor to client
    final String resultFile = System.getProperty("user.dir") + "/collectresult";

    final IRVertex collectVertex = new OperatorVertex(new CollectTransform<>(resultFile));
    builder.addVertex(collectVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, collectVertex),
        lastVertex, collectVertex);
    newEdge.setProperty(EncoderProperty.of(new SparkEncoderFactory(serializer)));
    newEdge.setProperty(DecoderProperty.of(new SparkDecoderFactory(serializer)));
    newEdge.setProperty(SPARK_KEY_EXTRACTOR_PROP);
    builder.connectVertices(newEdge);

    // launch DAG
    JobLauncher.launchDAG(builder.build());

    // Retrieve result data from file.
    // TODO #16: Implement collection of data from executor to client
    try {
      final List<T> result = new ArrayList<>();
      Integer i = 0;

      // TODO #16: Implement collection of data from executor to client
      File file = new File(resultFile + i);
      while (file.exists()) {
        try (
            final FileInputStream fis = new FileInputStream(file);
            final ObjectInputStream dis = new ObjectInputStream(fis)
        ) {
          final int size = dis.readInt(); // Read the number of collected T recorded in CollectTransform.
          for (int j = 0; j < size; j++) {
            result.add((T) dis.readObject());
          }
        }

        // Delete temporary file
        if (file.delete()) {
          file = new File(resultFile + ++i);
        }
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve communication pattern of the edge.
   *
   * @param src source vertex.
   * @param dst destination vertex.
   * @return the communication pattern.
   */
  public static DataCommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
                                                                                   final IRVertex dst) {
    if (dst instanceof OperatorVertex
        && (((OperatorVertex) dst).getTransform() instanceof ReduceByKeyTransform
        || ((OperatorVertex) dst).getTransform() instanceof GroupByKeyTransform)) {
      return DataCommunicationPatternProperty.Value.Shuffle;
    } else {
      return DataCommunicationPatternProperty.Value.OneToOne;
    }
  }

  /**
   * Converts a {@link Function1} to a corresponding {@link Function}.
   *
   * @param scalaFunction the scala function to convert.
   * @param <I>           the type of input.
   * @param <O>           the type of output.
   * @return the converted Java function.
   */
  public static <I, O> Function<I, O> toJavaFunction(final Function1<I, O> scalaFunction) {
    return new Function<I, O>() {
      @Override
      public O call(final I v1) throws Exception {
        return scalaFunction.apply(v1);
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

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
package edu.snu.nemo.compiler.frontend.spark.core.rdd;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.InitializedSourceVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor;
import edu.snu.nemo.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils;
import edu.snu.nemo.compiler.frontend.spark.source.SparkDatasetBoundedSourceVertex;
import edu.snu.nemo.compiler.frontend.spark.source.SparkTextFileBoundedSourceVertex;
import edu.snu.nemo.compiler.frontend.spark.sql.Dataset;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import edu.snu.nemo.compiler.frontend.spark.transform.*;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.TraversableOnce;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.List;
import java.util.Stack;

import static edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils.getEdgeCommunicationPattern;

/**
 * RDD for Nemo.
 * @param <T> type of data.
 */
public final class RDD<T> extends org.apache.spark.rdd.RDD<T> {
  private final SparkContext sparkContext;
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;
  private final IRVertex lastVertex;
  private final org.apache.spark.serializer.Serializer serializer;

  /**
   * Static method to create a RDD object from an iterable object.
   *
   * @param sparkContext spark context containing configurations.
   * @param initialData  initial data.
   * @param parallelism  parallelism information.
   * @param <T>          type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> RDD<T> of(final SparkContext sparkContext,
                              final Iterable<T> initialData,
                              final Integer parallelism) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    final IRVertex initializedSourceVertex = new InitializedSourceVertex<>(initialData);
    initializedSourceVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(initializedSourceVertex);

    return new RDD<>(sparkContext, builder.buildWithoutSourceSinkCheck(), initializedSourceVertex);
  }

  /**
   * Static method to create a JavaRDD object from an text file.
   *
   * @param sparkContext  the spark context containing configurations.
   * @param minPartitions the minimum number of partitions.
   * @param inputPath     the path of the input text file.
   * @param <T>           the type of resulting object.
   * @return the new JavaRDD object
   */
  public static <T> RDD<T> of(final SparkContext sparkContext,
                              final int minPartitions,
                              final String inputPath) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    final int numPartitions = sparkContext.textFile(inputPath, minPartitions).getNumPartitions();
    final IRVertex textSourceVertex = new SparkTextFileBoundedSourceVertex(sparkContext, inputPath, numPartitions);
    textSourceVertex.setProperty(ParallelismProperty.of(numPartitions));
    builder.addVertex(textSourceVertex);

    return new RDD<>(sparkContext, builder.buildWithoutSourceSinkCheck(), textSourceVertex);
  }

  /**
   * Static method to create a JavaRDD object from a Dataset.
   *
   * @param sparkSession spark session containing configurations.
   * @param dataset      dataset to read initial data from.
   * @param <T>          type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> RDD<T> of(final SparkSession sparkSession,
                              final Dataset<T> dataset) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    final IRVertex sparkBoundedSourceVertex = new SparkDatasetBoundedSourceVertex<>(sparkSession, dataset);
    sparkBoundedSourceVertex.setProperty(ParallelismProperty.of(dataset.rdd().getNumPartitions()));
    builder.addVertex(sparkBoundedSourceVertex);

    return new RDD<>(sparkSession.sparkContext(), builder.buildWithoutSourceSinkCheck(), sparkBoundedSourceVertex);
  }

  /**
   * Constructor.
   *
   * @param sparkContext spark context containing configurations.
   * @param dag          the current DAG.
   * @param lastVertex   last vertex added to the builder.
   */
  private RDD(final SparkContext sparkContext, final DAG<IRVertex, IREdge> dag, final IRVertex lastVertex) {
    super(sparkContext, null, ClassTag$.MODULE$.apply((Class<T>) Object.class));

    this.loopVertexStack = new Stack<>();
    this.sparkContext = sparkContext;
    this.dag = dag;
    this.lastVertex = lastVertex;
    this.serializer = SparkFrontendUtils.deriveSerializerFrom(sparkContext);
  }

  /**
   * @see org.apache.spark.rdd.RDD#compute(Partition, TaskContext).
   */
  @Override
  public Iterator<T> compute(final Partition partition, final TaskContext taskContext) {
    throw new UnsupportedOperationException("Operation unsupported.");
  }

  /**
   * @see org.apache.spark.rdd.RDD#getPartitions().
   */
  @Override
  public Partition[] getPartitions() {
    throw new UnsupportedOperationException("Operation unsupported.");
  }

  /////////////// Wrapping Scala functions ///////////////

  /**
   * @see org.apache.spark.rdd.RDD#map(Function1, ClassTag).
   */
  @Override
  public <O> RDD<O> map(final Function1<T, O> scalaFunction,
                        final scala.reflect.ClassTag<O> evidence$3) {
    return this.map(SparkFrontendUtils.toJavaFunction(scalaFunction));
  }

  /**
   * @see org.apache.spark.rdd.RDD#flatMap(Function1, ClassTag).
   */
  @Override
  public <O> RDD<O> flatMap(final Function1<T, TraversableOnce<O>> scalaFunction,
                            final scala.reflect.ClassTag<O> evidence$3) {
    return this.flatMap(SparkFrontendUtils.toFlatMapFunction(scalaFunction));
  }

  /**
   * @see org.apache.spark.rdd.RDD#collect().
   */
  @Override
  public T[] collect() {
    return (T[]) SparkFrontendUtils.collect(dag, loopVertexStack, lastVertex, serializer).toArray();
  }

  /**
   * @see org.apache.spark.rdd.RDD#reduce(scala.Function2).
   */
  @Override
  public T reduce(final scala.Function2<T, T, T> func) {
    return this.reduce(SparkFrontendUtils.toJavaFunction(func));
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Map transform.
   *
   * @param func function to apply.
   * @param <O>  output type.
   * @return the RDD with the extended DAG.
   */
  <O> RDD<O> map(final Function<T, O> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex mapVertex = new OperatorVertex(new MapTransform<>(func));
    builder.addVertex(mapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, mapVertex),
        lastVertex, mapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new RDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), mapVertex);
  }

  /**
   * Flat map transform.
   *
   * @param func function to apply.
   * @param <O>  output type.
   * @return the RDD with the extended DAG.
   */
  <O> RDD<O> flatMap(final FlatMapFunction<T, O> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex flatMapVertex = new OperatorVertex(new FlatMapTransform<>(func));
    builder.addVertex(flatMapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, flatMapVertex),
        lastVertex, flatMapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new RDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), flatMapVertex);
  }

  /////////////// TRANSFORMATION TO PAIR RDD ///////////////

  /**
   * Maps all elements in this RDD to pairs.
   *
   * @param f   the pair function.
   * @param <K> the type of key.
   * @param <V> the type of value.
   * @return the RDD of key-value pairs.
   */
  <K, V> JavaPairRDD<K, V> mapToPair(final PairFunction<T, K, V> f) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex mapToPairVertex = new OperatorVertex(new MapToPairTransform<>(f));
    builder.addVertex(mapToPairVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, mapToPairVertex),
        lastVertex, mapToPairVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaPairRDD<>(this.sparkContext, builder.buildWithoutSourceSinkCheck(), mapToPairVertex);
  }

  /////////////// ACTIONS ///////////////

  /**
   * Reduce action.
   *
   * @param func function (binary operator) to apply.
   * @return the result of the reduce action.
   */
  T reduce(final Function2<T, T, T> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex reduceVertex = new OperatorVertex(new ReduceTransform<>(func));
    builder.addVertex(reduceVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, reduceVertex),
        lastVertex, reduceVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return ReduceTransform.reduceIterator(collectToList().iterator(), func);
  }

  /**
   * Collect action.
   *
   * @return the collected data list.
   */
  List<T> collectToList() {
    return SparkFrontendUtils.collect(dag, loopVertexStack, lastVertex, serializer);
  }

  /**
   * @see org.apache.spark.rdd.RDD#saveAsTextFile(String).
   */
  @Override
  public void saveAsTextFile(String path) {
    // Check if given path is HDFS path.
    final boolean isHDFSPath = path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("file://");
    final Transform textFileTransform = isHDFSPath
        ? new HDFSTextFileTransform(path) : new LocalTextFileTransform(path);

    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex flatMapVertex = new OperatorVertex(textFileTransform);
    builder.addVertex(flatMapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, flatMapVertex),
        lastVertex, flatMapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    JobLauncher.launchDAG(builder.build());
  }
}

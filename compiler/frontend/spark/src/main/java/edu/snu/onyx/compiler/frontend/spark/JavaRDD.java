/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.compiler.frontend.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.InitializedSourceVertex;
import edu.snu.onyx.common.ir.vertex.LoopVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.onyx.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.onyx.compiler.frontend.spark.transform.MapTransform;
import edu.snu.onyx.compiler.frontend.spark.transform.ReduceTransform;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Java RDD.
 * @param <T> type of the final element.
 */
public final class JavaRDD<T> extends org.apache.spark.api.java.JavaRDD<T> {
  private final SparkContext sparkContext;
  private final Integer parallelism;
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;
  @Nullable private final IRVertex lastVertex;
  private final Serializer serializer;

  /**
   * Static method to create a JavaRDD object.
   * @param sparkContext spark context containing configurations.
   * @param parallelism parallelism information.
   * @param <T> type of the resulting object.
   * @return the new JavaRDD object.
   */
  static <T> JavaRDD<T> of(final SparkContext sparkContext, final Integer parallelism) {
    return new JavaRDD<>(sparkContext, parallelism,
        new DAGBuilder<IRVertex, IREdge>().buildWithoutSourceSinkCheck(), null);
  }

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param parallelism parallelism information.
   * @param dag the current DAG.
   * @param lastVertex last vertex added to the builder.
   */
  private JavaRDD(final SparkContext sparkContext, final Integer parallelism,
                  final DAG<IRVertex, IREdge> dag, @Nullable final IRVertex lastVertex) {
    // TODO #366: resolve while implementing scala RDD.
    super(null, null);

    this.loopVertexStack = new Stack<>();
    this.sparkContext = sparkContext;
    this.parallelism = parallelism;
    this.dag = dag;
    this.lastVertex = lastVertex;
    if (sparkContext.conf().get("spark.serializer", "")
        .equals("org.apache.spark.serializer.KryoSerializer")) {
      this.serializer = new KryoSerializer(sparkContext.conf());
    } else {
      this.serializer = new JavaSerializer(sparkContext.conf());
    }
  }

  /**
   * @return the spark context.
   */
  public SparkContext getSparkContext() {
    return sparkContext;
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Set initialized source.
   * @param initialData initial data.
   * @return the Java RDD with the initialized source vertex.
   */
  JavaRDD<T> setSource(final Iterable<T> initialData) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex initializedSourceVertex = new InitializedSourceVertex<>(initialData);
    initializedSourceVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(initializedSourceVertex, loopVertexStack);

    return new JavaRDD<>(this.sparkContext, this.parallelism,
        builder.buildWithoutSourceSinkCheck(), initializedSourceVertex);
  }

  /**
   * Map transform.
   * @param func function to apply.
   * @param <O> output type.
   * @return the JavaRDD with the DAG.
   */
  @Override
  public <O> JavaRDD<O> map(final Function<T, O> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex mapVertex = new OperatorVertex(new MapTransform<>(func));
    mapVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(mapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapVertex),
        lastVertex, mapVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaRDD<>(this.sparkContext, this.parallelism, builder.buildWithoutSourceSinkCheck(), mapVertex);
  }

  /////////////// ACTIONS ///////////////

  private static final AtomicInteger RESULT_ID = new AtomicInteger(0);

  /**
   * This method is to be removed after a result handler is implemented.
   * @return a unique integer.
   */
  public static Integer getResultId() {
    return RESULT_ID.getAndIncrement();
  }

  /**
   * Reduce action.
   * @param func function (binary operator) to apply.
   * @return the result of the reduce action.
   */
  @Override
  public T reduce(final Function2<T, T, T> func) {
    // save result in a temporary file
    final String resultFile = System.getProperty("user.dir") + "/reduceresult";
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex reduceVertex = new OperatorVertex(new ReduceTransform<>(func, resultFile));
    reduceVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(reduceVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, reduceVertex),
        lastVertex, reduceVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    // launch DAG
    JobLauncher.launchDAG(builder.build());

    // Retrieve result data from file.
    // TODO #711: remove this part, and make it properly write to sink.
    try {
      final Kryo kryo = new Kryo();
      final List<T> result = new ArrayList<>();
      Integer i = 0;
      // TODO #711: remove this part, and make it properly write to sink.
      File file = new File(resultFile + i);
      while (file.exists()) {
        final Input input = new Input(new FileInputStream(resultFile + i));
        result.add((T) kryo.readClassAndObject(input));
        input.close();

        // Delete temporary file
        file.delete();
        file = new File(resultFile + ++i);
      }
      return ReduceTransform.reduceIterator(result.iterator(), func);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /////////////// MISC ///////////////

  /**
   * Retrieve communication pattern of the edge.
   * @param src source vertex.
   * @param dst destination vertex.
   * @return the communication pattern.
   */
  private static DataCommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
                                                                                    final IRVertex dst) {
    // TODO #711: add Shuffle for KV data.
    return DataCommunicationPatternProperty.Value.OneToOne;
  }

  @Override
  public org.apache.spark.SparkContext context() {
    throw new UnsupportedOperationException("Operation unsupported. use getContext() instead.");
  }

  /////////////// UNSUPPORTED TRANSFORMATIONS ///////////////

  @Override
  public JavaRDD<T> cache() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> coalesce(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> coalesce(final int numPartitions, final boolean shuffle) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> distinct() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> distinct(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> filter(final Function<T, Boolean> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaRDD<U> flatMap(final FlatMapFunction<T, U> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<List<T>> glom() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaRDD<U> mapPartitions(final FlatMapFunction<Iterator<T>, U> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> JavaRDD<U> mapPartitions(final FlatMapFunction<Iterator<T>, U> f, final boolean preservesPartitioning) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <R> JavaRDD<R> mapPartitionsWithIndex(final Function2<Integer, Iterator<T>, Iterator<R>> f,
                                                final boolean preservesPartitioning) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> persist(final StorageLevel newLevel) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T>[] randomSplit(final double[] weights) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T>[] randomSplit(final double[] weights, final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> repartition(final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> sample(final boolean withReplacement, final double fraction) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> sample(final boolean withReplacement, final double fraction, final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> setName(final String name) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <S> JavaRDD<T> sortBy(final Function<T, S> f, final boolean ascending, final int numPartitions) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> unpersist() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaRDD<T> unpersist(final boolean blocking) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  /////////////// UNSUPPORTED ACTIONS ///////////////

  @Override
  public <U> U aggregate(final U zeroValue, final Function2<U, T, U> seqOp, final Function2<U, U, U> combOp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void checkpoint() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> collect() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<List<T>> collectAsync()  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T>[] collectPartitions(final int[] partitionIds) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public long count() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<BoundedDouble> countApprox(final long timeout)  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<BoundedDouble> countApprox(final long timeout, final double confidence) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public long countApproxDistinct(final double relativeSD)  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<Long> countAsync()  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Map<T, Long> countByValue() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<Map<T, BoundedDouble>> countByValueApprox(final long timeout) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public PartialResult<Map<T, BoundedDouble>> countByValueApprox(final long timeout, final double confidence) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T first() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T fold(final T zeroValue, final Function2<T, T, T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void foreach(final VoidFunction<T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<Void> foreachAsync(final VoidFunction<T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void foreachPartition(final VoidFunction<Iterator<T>> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<Void> foreachPartitionAsync(final VoidFunction<Iterator<T>> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Optional<String> getCheckpointFile() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public int getNumPartitions()  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public StorageLevel getStorageLevel() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public int id() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public boolean isCheckpointed() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public boolean isEmpty()  {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Iterator<T> iterator(final Partition split, final TaskContext taskContext) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T max(final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T min(final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public String name() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public org.apache.spark.api.java.Optional<Partitioner> partitioner() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<Partition> partitions() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void saveAsObjectFile(final String path) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void saveAsTextFile(final String path) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public void saveAsTextFile(final String path,
                              final Class<? extends org.apache.hadoop.io.compress.CompressionCodec> codec) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> take(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public JavaFutureAction<List<T>> takeAsync(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeOrdered(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeOrdered(final int num, final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeSample(final boolean withReplacement, final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> takeSample(final boolean withReplacement, final int num, final long seed) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public String toDebugString() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public Iterator<T> toLocalIterator() {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> top(final int num) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public List<T> top(final int num, final Comparator<T> comp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> U treeAggregate(final U zeroValue, final Function2<U, T, U> seqOp, final Function2<U, U, U> combOp) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public <U> U treeAggregate(final U zeroValue, final Function2<U, T, U> seqOp,
                              final Function2<U, U, U> combOp, final int depth) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T treeReduce(final Function2<T, T, T> f) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  @Override
  public T treeReduce(final Function2<T, T, T> f, final int depth) {
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }
}

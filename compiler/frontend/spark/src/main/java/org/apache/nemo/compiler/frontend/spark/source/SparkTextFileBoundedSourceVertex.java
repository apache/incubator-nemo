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
package org.apache.nemo.compiler.frontend.spark.source;

import org.apache.nemo.common.ir.BoundedIteratorReadable;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.SizeEstimator;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Bounded source vertex for Spark text file.
 */
public final class SparkTextFileBoundedSourceVertex extends SourceVertex<String> {
  private List<Readable<String>> readables;
  private long estimatedSizeBytes;

  /**
   * Constructor.
   *
   * @param sparkContext  the spark context.
   * @param inputPath     the path of the target text file.
   * @param numPartitions the number of partitions.
   */
  public SparkTextFileBoundedSourceVertex(final SparkContext sparkContext,
                                          final String inputPath,
                                          final int numPartitions) {
    this.readables = new ArrayList<>();
    final Partition[] partitions = sparkContext.textFile(inputPath, numPartitions).getPartitions();
    for (int i = 0; i < partitions.length; i++) {
      readables.add(new SparkTextFileBoundedSourceReadable(
        partitions[i],
        sparkContext.getConf(),
        i,
        inputPath,
        numPartitions));
    }
    this.estimatedSizeBytes = SizeEstimator.estimate(sparkContext.textFile(inputPath, numPartitions));
  }

  /**
   * Constructor for cloning.
   *
   * @param that the source object for copying
   */
  private SparkTextFileBoundedSourceVertex(final SparkTextFileBoundedSourceVertex that) {
    super(that);
    this.readables = new ArrayList<>();
    that.readables.forEach(this.readables::add);
  }

  @Override
  public SparkTextFileBoundedSourceVertex getClone() {
    return new SparkTextFileBoundedSourceVertex(this);
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public List<Readable<String>> getReadables(final int desiredNumOfSplits) {
    return readables;
  }

  @Override
  public String getSourceName() {
    return "SparkTextFileSource";
  }

  @Override
  public long getEstimatedSizeBytes() {
    return this.estimatedSizeBytes;
  }

  @Override
  public void clearInternalStates() {
    readables = null;
  }

  /**
   * A Readable wrapper for Spark text file.
   */
  private final class SparkTextFileBoundedSourceReadable extends BoundedIteratorReadable<String> {
    private final SparkConf sparkConf;
    private final int partitionIndex;
    private final List<String> locations;
    private final String inputPath;
    private final int numPartitions;

    /**
     * Constructor.
     *
     * @param partition      the partition to wrap.
     * @param sparkConf      configuration needed to build the SparkContext.
     * @param partitionIndex partition for this readable.
     * @param inputPath      the input file path.
     * @param numPartitions  the total number of partitions.
     */
    private SparkTextFileBoundedSourceReadable(final Partition partition,
                                               final SparkConf sparkConf,
                                               final int partitionIndex,
                                               final String inputPath,
                                               final int numPartitions) {
      this.sparkConf = sparkConf;
      this.partitionIndex = partitionIndex;
      this.inputPath = inputPath;
      this.numPartitions = numPartitions;
      this.locations = SparkSourceUtil.getPartitionLocation(partition);
    }

    @Override
    public long readWatermark() {
      throw new UnsupportedOperationException("No watermark");
    }

    @Override
    public List<String> getLocations() {
      if (locations.isEmpty()) {
        throw new UnsupportedOperationException();
      } else {
        return locations;
      }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected Iterator<String> initializeIterator() {
      // for setting up the same environment in the executors.
      final SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);

      // Spark does lazy evaluation: it doesn't load the full data in rdd, but only the partition it is asked for.
      final RDD<String> rdd = sparkContext.textFile(inputPath, numPartitions);
      final Iterable<String> iterable = () -> JavaConverters.asJavaIteratorConverter(
        rdd.iterator(rdd.getPartitions()[partitionIndex], TaskContext$.MODULE$.empty())).asJava();
      return iterable.iterator();
    }
  }
}

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
import org.apache.nemo.compiler.frontend.spark.sql.Dataset;
import org.apache.nemo.compiler.frontend.spark.sql.SparkSession;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext$;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.util.*;

/**
 * Bounded source vertex for Spark Dataset.
 *
 * @param <T> type of data to read.
 */
public final class SparkDatasetBoundedSourceVertex<T> extends SourceVertex<T> {
  private List<Readable<T>> readables;
  private long estimatedByteSize;

  /**
   * Constructor.
   *
   * @param sparkSession sparkSession to recreate on each executor.
   * @param dataset      Dataset to read data from.
   */
  public SparkDatasetBoundedSourceVertex(final SparkSession sparkSession, final Dataset<T> dataset) {
    this.readables = new ArrayList<>();
    final RDD rdd = dataset.sparkRDD();
    final Partition[] partitions = rdd.getPartitions();
    for (int i = 0; i < partitions.length; i++) {
      readables.add(new SparkDatasetBoundedSourceReadable(
        partitions[i],
        sparkSession.getDatasetCommandsList(),
        sparkSession.getInitialConf(),
        i));
    }
    this.estimatedByteSize = dataset.javaRDD()
      .map(o -> (long) o.toString().getBytes("UTF-8").length)
      .reduce((a, b) -> a + b);
  }

  /**
   * Copy Constructor for SparkDatasetBoundedSourceVertex.
   *
   * @param that the source object for copying
   */
  private SparkDatasetBoundedSourceVertex(final SparkDatasetBoundedSourceVertex<T> that) {
    super(that);
    this.readables = new ArrayList<>();
    that.readables.forEach(this.readables::add);
  }

  @Override
  public SparkDatasetBoundedSourceVertex getClone() {
    return new SparkDatasetBoundedSourceVertex(this);
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) {
    return readables;
  }

  @Override
  public String getSourceName() {
    return "SparkDatasetSource";
  }

  @Override
  public long getEstimatedSizeBytes() {
    return this.estimatedByteSize;
  }

  @Override
  public void clearInternalStates() {
    readables = null;
  }

  /**
   * A Readable wrapper for Spark Dataset.
   */
  private final class SparkDatasetBoundedSourceReadable extends BoundedIteratorReadable<T> {
    private final LinkedHashMap<String, Object[]> commands;
    private final Map<String, String> sessionInitialConf;
    private final int partitionIndex;
    private final List<String> locations;

    /**
     * Constructor.
     *
     * @param partition          the partition to wrap.
     * @param commands           list of commands needed to build the dataset.
     * @param sessionInitialConf spark session's initial configuration.
     * @param partitionIndex     partition for this readable.
     */
    private SparkDatasetBoundedSourceReadable(final Partition partition,
                                              final LinkedHashMap<String, Object[]> commands,
                                              final Map<String, String> sessionInitialConf,
                                              final int partitionIndex) {
      this.commands = commands;
      this.sessionInitialConf = sessionInitialConf;
      this.partitionIndex = partitionIndex;
      this.locations = SparkSourceUtil.getPartitionLocation(partition);
    }

    @Override
    protected Iterator<T> initializeIterator() {
      // for setting up the same environment in the executors.
      final SparkSession spark = SparkSession.builder()
        .config(sessionInitialConf)
        .getOrCreate();
      final Dataset<T> dataset;

      try {
        dataset = SparkSession.initializeDataset(spark, commands);
      } catch (final OperationNotSupportedException e) {
        throw new IllegalStateException(e);
      }

      // Spark does lazy evaluation: it doesn't load the full dataset, but only the partition it is asked for.
      final RDD<T> rdd = dataset.sparkRDD();
      final Iterable<T> iterable = () -> JavaConverters.asJavaIteratorConverter(
        rdd.iterator(rdd.getPartitions()[partitionIndex], TaskContext$.MODULE$.empty())).asJava();
      return iterable.iterator();
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
  }
}

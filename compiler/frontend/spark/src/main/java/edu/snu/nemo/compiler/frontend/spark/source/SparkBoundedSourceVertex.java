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
package edu.snu.nemo.compiler.frontend.spark.source;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.compiler.frontend.spark.sql.Dataset;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import org.apache.spark.TaskContext$;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final List<Readable<T>> readables;

  /**
   * Constructor.
   * Note that we have to first create our iterators here and supply them to our readables.
   *
   * @param sparkSession sparkSession to recreate on each executor.
   * @param dataset Dataset to read data from.
   */
  public SparkBoundedSourceVertex(final SparkSession sparkSession, final Dataset<T> dataset) {
    this.readables = new ArrayList<>();
    IntStream.range(0, dataset.rdd().getNumPartitions()).forEach(partitionIndex ->
        readables.add(new SparkBoundedSourceReadable(
            sparkSession.getDatasetCommandsList(),
            sparkSession.getInitialConf(),
            partitionIndex)));
  }

  /**
   * Constructor.
   *
   * @param readables the list of Readables to set.
   */
  public SparkBoundedSourceVertex(final List<Readable<T>> readables) {
    this.readables = readables;
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>((this.readables));
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) {
    return readables;
  }

  /**
   * A Readable for SparkBoundedSourceReadablesWrapper.
   */
  private final class SparkBoundedSourceReadable implements Readable<T> {
    private final LinkedHashMap<String, Object[]> commands;
    private final Map<String, String> sessionInitialConf;
    private final int partitionIndex;

    /**
     * Constructor.
     * @param commands list of commands needed to build the dataset.
     * @param sessionInitialConf spark session's initial configuration.
     * @param partitionIndex partition for this readable.
     */
    private SparkBoundedSourceReadable(final LinkedHashMap<String, Object[]> commands,
                                       final Map<String, String> sessionInitialConf,
                                       final int partitionIndex) {
      this.commands = commands;
      this.sessionInitialConf = sessionInitialConf;
      this.partitionIndex = partitionIndex;
    }

    @Override
    public Iterable<T> read() throws Exception {
      // for setting up the same environment in the executors.
      final SparkSession spark = SparkSession.builder()
          .config(sessionInitialConf)
          .getOrCreate();
      final Dataset<T> dataset = SparkSession.initializeDataset(spark, commands);

      // Spark does lazy evaluation: it doesn't load the full dataset, but only the partition it is asked for.
      final RDD<T> rdd = dataset.rdd();
      return () -> JavaConverters.asJavaIteratorConverter(
          rdd.iterator(rdd.getPartitions()[partitionIndex], TaskContext$.MODULE$.empty())).asJava();
    }

    @Override
    public List<String> getLocations() {
      throw new UnsupportedOperationException();
    }
  }
}

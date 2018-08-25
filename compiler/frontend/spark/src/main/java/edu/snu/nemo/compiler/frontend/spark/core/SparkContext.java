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

import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.InMemorySourceVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.compiler.frontend.spark.core.rdd.JavaRDD;
import edu.snu.nemo.compiler.frontend.spark.core.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Spark context wrapper for in Nemo.
 */
public final class SparkContext extends org.apache.spark.SparkContext {
  private static final Logger LOG = LoggerFactory.getLogger(SparkContext.class.getName());
  private final org.apache.spark.SparkContext sparkContext;

  /**
   * Constructor.
   */
  public SparkContext() {
    this.sparkContext = org.apache.spark.SparkContext.getOrCreate();
  }

  /**
   * Constructor with configuration.
   *
   * @param sparkConf spark configuration to wrap.
   */
  public SparkContext(final SparkConf sparkConf) {
    super(sparkConf);
    this.sparkContext = org.apache.spark.SparkContext.getOrCreate(sparkConf);
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   *
   * @param seq        input data as list.
   * @param numSlices  number of slices (parallelism).
   * @param evidence   type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  @Override
  public <T> RDD<T> parallelize(final Seq<T> seq,
                                final int numSlices,
                                final ClassTag<T> evidence) {
    final List<T> javaList = scala.collection.JavaConversions.seqAsJavaList(seq);
    return JavaRDD.of(this.sparkContext, javaList, numSlices).rdd();
  }

  @Override
  public <T> Broadcast<T> broadcast(final T data,
                                    final ClassTag<T> evidence) {
    // register this data (IRVertex -> SourceVertex?)
    // create a tag for the data
    final IRVertex inMemoryVertex = new InMemorySourceVertex<>(Collections.singleton(data));
    inMemoryVertex.setProperty(ParallelismProperty.of(1)); // singleton
    // builder.addVertex(inMemoryVertex);

    final HashMap serializableHashMap = new HashMap();
    return new SparkBroadcast(99999, serializableHashMap, serializableHashMap.getClass());
  }
}

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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import java.util.Stack;

/**
 * RDD for Nemo.
 * @param <T> type of data.
 */
public final class RDD<T> extends org.apache.spark.rdd.RDD<T> {
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;

  /**
   * Static method to create a RDD object.
   * @param sparkContext spark context containing configurations.
   * @param <T> type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> RDD<T> of(final SparkContext sparkContext) {
    return new RDD<>(sparkContext, new DAGBuilder<IRVertex, IREdge>().buildWithoutSourceSinkCheck());
  }

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param dag the current DAG.
   */
  private RDD(final SparkContext sparkContext, final DAG<IRVertex, IREdge> dag) {
    super(sparkContext, null, ClassTag$.MODULE$.apply((Class<T>) Object.class));

    this.loopVertexStack = new Stack<>();
    this.dag = dag;
  }

  @Override
  public Iterator<T> compute(final Partition partition, final TaskContext taskContext) {
    throw new UnsupportedOperationException("Operation unsupported.");
  }

  @Override
  public Partition[] getPartitions() {
    throw new UnsupportedOperationException("Operation unsupported.");
  }
}

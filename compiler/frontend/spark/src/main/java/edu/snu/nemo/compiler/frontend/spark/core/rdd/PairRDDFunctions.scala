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
package edu.snu.nemo.compiler.frontend.spark.core.rdd

import java.util

import edu.snu.nemo.common.dag.DAGBuilder
import edu.snu.nemo.common.ir.edge.IREdge
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty
import edu.snu.nemo.common.ir.vertex.{IRVertex, LoopVertex, OperatorVertex}
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor
import edu.snu.nemo.compiler.frontend.spark.coder.SparkCoder
import edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils
import edu.snu.nemo.compiler.frontend.spark.transform.ReduceByKeyTransform

import scala.reflect.ClassTag

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion in Nemo.
 */
final class PairRDDFunctions[K: ClassTag, V: ClassTag] protected[rdd] (
    self: RDD[(K, V)]) extends org.apache.spark.rdd.PairRDDFunctions[K, V](self) {

  private val loopVertexStack = new util.Stack[LoopVertex]

  /////////////// TRANSFORMATIONS ///////////////

  /**
    * Merge the values for each key using an associative and commutative reduce function. This will
    * also perform the merging locally on each mapper before sending results to a reducer, similarly
    * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
    * parallelism level.
    */
  override def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    val javaFunc = SparkFrontendUtils.toJavaFunction(func)
    val builder = new DAGBuilder[IRVertex, IREdge](self.dag)

    val reduceByKeyVertex = new OperatorVertex(new ReduceByKeyTransform[K, V](javaFunc))
    builder.addVertex(reduceByKeyVertex, loopVertexStack)

    val newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(self.lastVertex, reduceByKeyVertex),
      self.lastVertex, reduceByKeyVertex, new SparkCoder[Tuple2[K, V]](self.serializer))
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor))
    builder.connectVertices(newEdge)

    new RDD[(K, V)](self._sc, builder.buildWithoutSourceSinkCheck, reduceByKeyVertex, Option.empty)
  }
}

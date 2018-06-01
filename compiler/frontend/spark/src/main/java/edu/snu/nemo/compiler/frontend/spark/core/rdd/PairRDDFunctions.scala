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

import edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils

import scala.reflect.ClassTag

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion in Nemo.
 */
final class PairRDDFunctions[K: ClassTag, V: ClassTag] (
    self: RDD[(K, V)],
    private val javaPairRDD: JavaPairRDD[K, V]) extends org.apache.spark.rdd.PairRDDFunctions[K, V](self) {

  /**
    * @return converted JavaRDD.
    */
  def toJavaPairRDD() : JavaPairRDD[K, V] = {
    javaPairRDD
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
    * Merge the values for each key using an associative and commutative reduce function. This will
    * also perform the merging locally on each mapper before sending results to a reducer, similarly
    * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
    * parallelism level.
    */
  override def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    javaPairRDD.reduceByKey(SparkFrontendUtils.toJavaFunction(func)).rdd()
  }
}

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
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * RDD for Nemo.
 */
final class RDD[T: ClassTag] protected (
    private val _sc: SparkContext,
    private val deps: Seq[Dependency[_]],
    private val javaRDD: JavaRDD[T]) extends org.apache.spark.rdd.RDD[T](_sc, deps) {

  /**
   * Constructor without dependencies (not needed in Nemo RDD).
   *
   * @param sparkContext the spark context.
   * @param javaRDDFrom  the java RDD.
   */
  protected def this(sparkContext: SparkContext, javaRDDFrom: JavaRDD[T]) = {
    this(sparkContext, Nil, javaRDDFrom)
  }

  protected def this(sparkContext: SparkContext) = {
    this(sparkContext, Nil, null) // TODO: TMP
  }

  /**
   * @return converted JavaRDD.
   */
  override def toJavaRDD() : JavaRDD[T] = {
    javaRDD
  }

  /**
   * Not supported yet.
   */
  override def getPartitions: Array[Partition] = {
    throw new UnsupportedOperationException("Operation unsupported.")
  }

  /**
   * Not supported yet.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    throw new UnsupportedOperationException("Operation unsupported.")
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  override def map[U](f: (T) => U)(implicit evidence$3: ClassManifest[U]): RDD[U] = {
    new RDD[U](_sc, deps, javaRDD.map(SparkFrontendUtils.toJavaFunction(f)))
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    new RDD[U](_sc, deps, javaRDD.flatMap(SparkFrontendUtils.toFlatMapFunction(f)))
  }

  /////////////// ACTIONS ///////////////

  /**
   * Return an array that contains all of the elements in this RDD.
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  override def collect(): Array[T] = {
    javaRDD.collect().toArray().asInstanceOf[Array[T]]
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   */
  override def reduce(f: (T, T) => T): T = {
    javaRDD.reduce(SparkFrontendUtils.toJavaFunction(f))
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  override def saveAsTextFile(path: String): Unit = {
    javaRDD.saveAsTextFile(path)
  }
}

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

import edu.snu.nemo.client.JobLauncher
import edu.snu.nemo.common.dag.{DAG, DAGBuilder}
import edu.snu.nemo.common.ir.edge.IREdge
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty
import edu.snu.nemo.common.ir.vertex.{IRVertex, LoopVertex, OperatorVertex}
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor
import edu.snu.nemo.compiler.frontend.spark.coder.SparkCoder
import edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils
import edu.snu.nemo.compiler.frontend.spark.transform._
import org.apache.hadoop.io.WritableFactory
import org.apache.spark.rdd.{AsyncRDDActions, DoubleRDDFunctions, OrderedRDDFunctions, SequenceFileRDDFunctions}
import org.apache.spark.serializer.Serializer
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.function.{FlatMapFunction, Function, Function2}

import scala.reflect.ClassTag
import scala.language.implicitConversions

/**
 * RDD for Nemo.
 */
final class RDD[T: ClassTag] protected[rdd] (
    protected[rdd] val _sc: SparkContext,
    private val deps: Seq[Dependency[_]],
    protected[rdd] val dag: DAG[IRVertex, IREdge],
    protected[rdd] val lastVertex: IRVertex,
    private val sourceRDD: Option[org.apache.spark.rdd.RDD[T]]) extends org.apache.spark.rdd.RDD[T](_sc, deps) {

  private val loopVertexStack = new util.Stack[LoopVertex]
  protected[rdd] val serializer: Serializer = SparkFrontendUtils.deriveSerializerFrom(_sc)

  /**
   * Constructor without dependencies (not needed in Nemo RDD).
   *
   * @param sparkContext the spark context.
   */
  protected[rdd] def this(sparkContext: SparkContext,
                          dagFrom: DAG[IRVertex, IREdge],
                          lastVertexFrom: IRVertex,
                          sourceRDDFrom: Option[org.apache.spark.rdd.RDD[T]]) = {
    this(sparkContext, Nil, dagFrom, lastVertexFrom, sourceRDDFrom)
  }

  /**
   * @return converted JavaRDD.
   */
  override def toJavaRDD() : JavaRDD[T] = {
    new JavaRDD[T](this)
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

  /////////////// WRAPPER FUNCTIONS /////////////

  /**
   * A scala wrapper for map transformation.
   */
  override def map[U](f: (T) => U)(implicit evidence$3: ClassManifest[U]): RDD[U] = {
    val javaFunc = SparkFrontendUtils.toJavaFunction(f)
    map(javaFunc)
  }

  /**
   * A scala wrapper for flatMap transformation.
   */
  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    val javaFunc = SparkFrontendUtils.toFlatMapFunction(f)
    flatMap(javaFunc)
  }

  /**
   * A scala wrapper for reduce action.
   */
  override def reduce(f: (T, T) => T): T = {
    val javaFunc = SparkFrontendUtils.toJavaFunction(f)
    reduce(javaFunc)
  }

  /**
   * A scala wrapper for collect action.
   *
   * @return the collected value.
   * @note This method should only be used if the resulting array is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  override def collect(): Array[T] =
    collectAsList().toArray().asInstanceOf[Array[T]]

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  protected[rdd] def map[U: ClassTag](javaFunc: Function[T, U]): RDD[U] = {
    val builder: DAGBuilder[IRVertex, IREdge] = new DAGBuilder[IRVertex, IREdge](dag)

    val mapVertex: IRVertex = new OperatorVertex(new MapTransform[T, U](javaFunc))
    builder.addVertex(mapVertex, loopVertexStack)

    val newEdge: IREdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, mapVertex),
      lastVertex, mapVertex, new SparkCoder[T](serializer))
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor))
    builder.connectVertices(newEdge)

    new RDD[U](_sc, builder.buildWithoutSourceSinkCheck, mapVertex, Option.empty)
  }

  /**
   * Return a new RDD by first applying a function to all elements of this
   * RDD, and then flattening the results.
   */
  protected[rdd] def flatMap[U: ClassTag](javaFunc: FlatMapFunction[T, U]): RDD[U] = {
    val builder = new DAGBuilder[IRVertex, IREdge](dag)

    val flatMapVertex = new OperatorVertex(new FlatMapTransform[T, U](javaFunc))
    builder.addVertex(flatMapVertex, loopVertexStack)

    val newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, flatMapVertex),
      lastVertex, flatMapVertex, new SparkCoder[T](serializer))
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor))
    builder.connectVertices(newEdge)

    new RDD[U](_sc, builder.buildWithoutSourceSinkCheck, flatMapVertex, Option.empty)
  }

  /////////////// ACTIONS ///////////////

  /**
   * Return a list that contains all of the elements in this RDD.
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  protected[rdd] def collectAsList(): util.List[T] =
    SparkFrontendUtils.collect(dag, loopVertexStack, lastVertex, serializer)

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   */
  protected[rdd] def reduce(javaFunc: Function2[T, T, T]): T = {
    val builder = new DAGBuilder[IRVertex, IREdge](dag)

    val reduceVertex = new OperatorVertex(new ReduceTransform[T](javaFunc))
    builder.addVertex(reduceVertex, loopVertexStack)

    val newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, reduceVertex),
      lastVertex, reduceVertex, new SparkCoder[T](serializer))
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor))

    builder.connectVertices(newEdge)
    ReduceTransform.reduceIterator(
      SparkFrontendUtils.collect(dag, loopVertexStack, lastVertex, serializer).iterator(), javaFunc)
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  override def saveAsTextFile(path: String): Unit = {
    // Check if given path is HDFS path.
    val isHDFSPath = path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("file://")
    val textFileTransform =
      if (isHDFSPath) new HDFSTextFileTransform[T](path)
      else new LocalTextFileTransform[T](path)

    val builder = new DAGBuilder[IRVertex, IREdge](dag)
    val flatMapVertex = new OperatorVertex(textFileTransform)

    builder.addVertex(flatMapVertex, loopVertexStack)
    val newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(lastVertex, flatMapVertex),
      lastVertex, flatMapVertex, new SparkCoder[T](serializer))
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor))

    builder.connectVertices(newEdge)
    JobLauncher.launchDAG(builder.build)
  }
}

/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 *
 * For example, [[RDD.rddToPairRDDFunctions]] converts an RDD into a [[PairRDDFunctions]] for
 * key-value-pair RDDs, and enabling extra functionalities such as `PairRDDFunctions.reduceByKey`.
 */
object RDD {
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    throw new UnsupportedOperationException("Operation unsupported.")
  }

  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V],
     keyWritableFactory: WritableFactory,
     valueWritableFactory: WritableFactory): SequenceFileRDDFunctions[K, V] = {
    throw new UnsupportedOperationException("Operation unsupported.")
  }

  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
  : OrderedRDDFunctions[K, V, (K, V)] = {
    throw new UnsupportedOperationException("Operation unsupported.")
  }

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    throw new UnsupportedOperationException("Operation unsupported.")
  }

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T])
  : DoubleRDDFunctions = {
    throw new UnsupportedOperationException("Operation unsupported.")
  }
}

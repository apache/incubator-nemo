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
import edu.snu.nemo.common.ir.edge.executionproperty.{DecoderProperty, EncoderProperty, KeyExtractorProperty}
import edu.snu.nemo.common.ir.executionproperty.EdgeExecutionProperty
import edu.snu.nemo.common.ir.vertex.{IRVertex, LoopVertex, OperatorVertex}
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor
import edu.snu.nemo.compiler.frontend.spark.coder.{SparkDecoderFactory, SparkEncoderFactory}
import edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils
import edu.snu.nemo.compiler.frontend.spark.transform._
import org.apache.hadoop.io.WritableFactory
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.api.java.function.{FlatMapFunction, Function, Function2}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.{AsyncRDDActions, DoubleRDDFunctions, OrderedRDDFunctions, PartitionCoalescer, SequenceFileRDDFunctions}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, Partition, Partitioner, SparkContext, TaskContext}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * RDD for Nemo.
 */
final class RDD[T: ClassTag] protected[rdd] (
    protected[rdd] val _sc: SparkContext,
    private val deps: Seq[Dependency[_]],
    protected[rdd] val dag: DAG[IRVertex, IREdge],
    protected[rdd] val lastVertex: IRVertex,
    private val sourceRDD: Option[org.apache.spark.rdd.RDD[T]]) extends org.apache.spark.rdd.RDD[T](_sc, deps) {

  protected[rdd] val serializer: Serializer = SparkFrontendUtils.deriveSerializerFrom(_sc)
  private val loopVertexStack = new util.Stack[LoopVertex]
  private val encoderProperty: EdgeExecutionProperty[_ <: Serializable] =
    EncoderProperty.of(new SparkEncoderFactory[T](serializer)).asInstanceOf[EdgeExecutionProperty[_ <: Serializable]]
  private val decoderProperty: EdgeExecutionProperty[_ <: Serializable] =
    DecoderProperty.of(new SparkDecoderFactory[T](serializer)).asInstanceOf[EdgeExecutionProperty[_ <: Serializable]]
  private val keyExtractorProperty: KeyExtractorProperty = KeyExtractorProperty.of(new SparkKeyExtractor)

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
    val javaFunc = SparkFrontendUtils.toJavaFlatMapFunction(f)
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
      lastVertex, mapVertex)
    newEdge.setProperty(encoderProperty)
    newEdge.setProperty(decoderProperty)
    newEdge.setProperty(keyExtractorProperty)
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
      lastVertex, flatMapVertex)
    newEdge.setProperty(encoderProperty)
    newEdge.setProperty(decoderProperty)
    newEdge.setProperty(keyExtractorProperty)
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
      lastVertex, reduceVertex)
    newEdge.setProperty(encoderProperty)
    newEdge.setProperty(decoderProperty)
    newEdge.setProperty(keyExtractorProperty)

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
      lastVertex, flatMapVertex)
    newEdge.setProperty(encoderProperty)
    newEdge.setProperty(decoderProperty)
    newEdge.setProperty(keyExtractorProperty)

    builder.connectVertices(newEdge)
    JobLauncher.launchDAG(builder.build)
  }

  /////////////// UNSUPPORTED METHODS ///////////////
  //TODO#92: Implement the unimplemented transformations/actions & dataset initialization methods for Spark frontend.

  override protected def getDependencies: Seq[Dependency[_]] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def sparkContext: SparkContext = super.sparkContext

  override def setName(_name: String): RDD.this.type =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def persist(newLevel: StorageLevel): RDD.this.type =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def persist(): RDD.this.type =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cache(): RDD.this.type =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def unpersist(blocking: Boolean): RDD.this.type =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def getStorageLevel: StorageLevel =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def filter(f: (T) => Boolean): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def distinct(): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def coalesce(numPartitions: Int, shuffle: Boolean, partitionCoalescer: Option[PartitionCoalescer])
                       (implicit ord: Ordering[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def randomSplit(weights: Array[Double], seed: Long): Array[org.apache.spark.rdd.RDD[T]] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def union(other: org.apache.spark.rdd.RDD[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def ++(other: org.apache.spark.rdd.RDD[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)
                        (implicit ord: Ordering[K], ctag: ClassManifest[K]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def intersection(other: org.apache.spark.rdd.RDD[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def intersection(other: org.apache.spark.rdd.RDD[T], partitioner: Partitioner)
                           (implicit ord: Ordering[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def intersection(other: org.apache.spark.rdd.RDD[T], numPartitions: Int): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def glom(): RDD[Array[T]] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cartesian[U](other: org.apache.spark.rdd.RDD[U])(implicit evidence$5: ClassManifest[U]): RDD[(T, U)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupBy[K](f: (T) => K)(implicit kt: ClassManifest[K]): RDD[(K, Iterable[T])] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupBy[K](f: (T) => K, numPartitions: Int)(implicit kt: ClassManifest[K]): RDD[(K, Iterable[T])] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupBy[K](f: (T) => K, p: Partitioner)
                         (implicit kt: ClassManifest[K], ord: Ordering[K]): RDD[(K, Iterable[T])] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def pipe(command: String): RDD[String] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def pipe(command: String, env: scala.collection.Map[String, String]): RDD[String] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def pipe(command: Seq[String], env: scala.collection.Map[String, String],
                    printPipeContext: ((String) => Unit) => Unit,
                    printRDDElement: (T, (String) => Unit) => Unit, separateWorkingDir: Boolean,
                    bufferSize: Int, encoding: String): RDD[String] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def mapPartitions[U](f: (Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)
                               (implicit evidence$6: ClassManifest[U]): RDD[U] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def mapPartitionsWithIndex[U](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean)
                                        (implicit evidence$9: ClassManifest[U]): RDD[U] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zip[U](other: org.apache.spark.rdd.RDD[U])(implicit evidence$10: ClassManifest[U]): RDD[(T, U)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipPartitions[B, V](rdd2: org.apache.spark.rdd.RDD[B], preservesPartitioning: Boolean)
                                  (f: (Iterator[T], Iterator[B]) => Iterator[V])
                                  (implicit evidence$11: ClassManifest[B], evidence$12: ClassManifest[V]): RDD[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipPartitions[B, V](rdd2: org.apache.spark.rdd.RDD[B])(f: (Iterator[T], Iterator[B]) => Iterator[V])
                                  (implicit evidence$13: ClassManifest[B], evidence$14: ClassManifest[V]): RDD[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipPartitions[B, C, V](rdd2: org.apache.spark.rdd.RDD[B],
                                      rdd3: org.apache.spark.rdd.RDD[C], preservesPartitioning: Boolean)
                                     (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])
                                     (implicit evidence$15: ClassManifest[B], evidence$16: ClassManifest[C],
                                      evidence$17: ClassManifest[V]): RDD[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipPartitions[B, C, V](rdd2: org.apache.spark.rdd.RDD[B],
                                      rdd3: org.apache.spark.rdd.RDD[C])
                                     (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V])
                                     (implicit evidence$18: ClassManifest[B], evidence$19: ClassManifest[C],
                                      evidence$20: ClassManifest[V]): RDD[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipPartitions[B, C, D, V](rdd2: org.apache.spark.rdd.RDD[B],
                                         rdd3: org.apache.spark.rdd.RDD[C],
                                         rdd4: org.apache.spark.rdd.RDD[D], preservesPartitioning: Boolean)
                                        (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])
                                        (implicit evidence$21: ClassManifest[B], evidence$22: ClassManifest[C],
                                         evidence$23: ClassManifest[D], evidence$24: ClassManifest[V]): RDD[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipPartitions[B, C, D, V](rdd2: org.apache.spark.rdd.RDD[B],
                                         rdd3: org.apache.spark.rdd.RDD[C],
                                         rdd4: org.apache.spark.rdd.RDD[D])
                                        (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V])
                                        (implicit evidence$25: ClassManifest[B], evidence$26: ClassManifest[C],
                                         evidence$27: ClassManifest[D], evidence$28: ClassManifest[V]): RDD[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def foreach(f: (T) => Unit): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def toLocalIterator: Iterator[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def collect[U](f: PartialFunction[T, U])(implicit evidence$29: ClassManifest[U]): RDD[U] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def subtract(other: org.apache.spark.rdd.RDD[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def subtract(other: org.apache.spark.rdd.RDD[T], numPartitions: Int): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def subtract(other: org.apache.spark.rdd.RDD[T], p: Partitioner)(implicit ord: Ordering[T]): RDD[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def treeReduce(f: (T, T) => T, depth: Int): T =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def fold(zeroValue: T)(op: (T, T) => T): T =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def aggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
                           (implicit evidence$30: ClassManifest[U]): U =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def treeAggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U, depth: Int)
                               (implicit evidence$31: ClassManifest[U]): U =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def count(): Long =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countByValue()(implicit ord: Ordering[T]): Map[T, Long] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countByValueApprox(timeout: Long, confidence: Double)
                                 (implicit ord: Ordering[T]): PartialResult[scala.collection.Map[T, BoundedDouble]] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countApproxDistinct(p: Int, sp: Int): Long =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countApproxDistinct(relativeSD: Double): Long =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipWithIndex(): RDD[(T, Long)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def zipWithUniqueId(): RDD[(T, Long)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def take(num: Int): Array[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def first(): T =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def max()(implicit ord: Ordering[T]): T =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def min()(implicit ord: Ordering[T]): T =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def isEmpty(): Boolean =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsObjectFile(path: String): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def keyBy[K](f: (T) => K): RDD[(K, T)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def checkpoint(): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def localCheckpoint(): RDD.this.type =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def isCheckpointed: Boolean =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def getCheckpointFile: Option[String] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def firstParent[U](implicit evidence$32: ClassManifest[U]): RDD[U] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def parent[U](j: Int)(implicit evidence$33: ClassManifest[U]): RDD[U] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def context: SparkContext =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override protected def clearDependencies(): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def toDebugString: String =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def toString(): String =
    throw new UnsupportedOperationException("Operation not yet implemented.")
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

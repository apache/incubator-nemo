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
import edu.snu.nemo.common.ir.edge.executionproperty.{DecoderProperty, EncoderProperty, KeyExtractorProperty}
import edu.snu.nemo.common.ir.executionproperty.EdgeExecutionProperty
import edu.snu.nemo.common.ir.vertex.{IRVertex, LoopVertex, OperatorVertex}
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor
import edu.snu.nemo.compiler.frontend.spark.coder.{SparkDecoderFactory, SparkEncoderFactory}
import edu.snu.nemo.compiler.frontend.spark.core.SparkFrontendUtils
import edu.snu.nemo.compiler.frontend.spark.transform.ReduceByKeyTransform
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.spark.api.java.function.Function2
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.{Partitioner, rdd}

import scala.reflect.ClassTag

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion in Nemo.
 */
final class PairRDDFunctions[K: ClassTag, V: ClassTag] protected[rdd] (
    self: RDD[(K, V)]) extends org.apache.spark.rdd.PairRDDFunctions[K, V](self) {

  private val loopVertexStack = new util.Stack[LoopVertex]

  /////////////// WRAPPER FUNCTIONS /////////////

  /**
   * A scala wrapper for reduceByKey transformation.
   */
  override def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    val javaFunc = SparkFrontendUtils.toJavaFunction(func)
    reduceByKey(javaFunc)
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  protected[rdd] def reduceByKey(javaFunc: Function2[V, V, V]): RDD[(K, V)] = {
    val builder = new DAGBuilder[IRVertex, IREdge](self.dag)

    val reduceByKeyVertex = new OperatorVertex(new ReduceByKeyTransform[K, V](javaFunc))
    builder.addVertex(reduceByKeyVertex, loopVertexStack)

    val newEdge = new IREdge(SparkFrontendUtils.getEdgeCommunicationPattern(self.lastVertex, reduceByKeyVertex),
      self.lastVertex, reduceByKeyVertex)
    newEdge.setProperty(
      EncoderProperty.of(new SparkEncoderFactory[Tuple2[K, V]](self.serializer))
        .asInstanceOf[EdgeExecutionProperty[_ <: Serializable]])
    newEdge.setProperty(
      DecoderProperty.of(new SparkDecoderFactory[Tuple2[K, V]](self.serializer))
        .asInstanceOf[EdgeExecutionProperty[_ <: Serializable]])
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor))
    builder.connectVertices(newEdge)

    new RDD[(K, V)](self._sc, builder.buildWithoutSourceSinkCheck, reduceByKeyVertex, Option.empty)
  }

  /////////////// UNSUPPORTED METHODS ///////////////
  //TODO#92: Implement the unimplemented transformations/actions & dataset initialization methods for Spark frontend.
  override def combineByKeyWithClassTag[C](createCombiner: V => C, mergeValue: (C, V) => C,
                                           mergeCombiners: (C, C) => C, partitioner: Partitioner,
                                           mapSideCombine: Boolean, serializer: Serializer)
                                          (implicit ct: ClassTag[C]): RDD[(K, C)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C,
                               mergeCombiners: (C, C) => C, partitioner: Partitioner,
                               mapSideCombine: Boolean, serializer: Serializer): RDD[(K, C)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C,
                               mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def combineByKeyWithClassTag[C](createCombiner: V => C, mergeValue: (C, V) => C,
                                           mergeCombiners: (C, C) => C, numPartitions: Int)
                                          (implicit ct: ClassTag[C]): RDD[(K, C)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def aggregateByKey[U](zeroValue: U, partitioner: Partitioner)
                                (seqOp: (U, V) => U, combOp: (U, U) => U)
                                (implicit evidence$1: ClassTag[U]): RDD[(K, U)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def aggregateByKey[U](zeroValue: U, numPartitions: Int)
                                (seqOp: (U, V) => U, combOp: (U, U) => U)
                                (implicit evidence$2: ClassTag[U]): RDD[(K, U)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def aggregateByKey[U](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U)
                                (implicit evidence$3: ClassTag[U]): RDD[(K, U)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def sampleByKey(withReplacement: Boolean,
                           fractions: collection.Map[K, Double], seed: Long): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def sampleByKeyExact(withReplacement: Boolean,
                                fractions: collection.Map[K, Double], seed: Long): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def reduceByKeyLocally(func: (V, V) => V): collection.Map[K, V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countByKey(): collection.Map[K, Long] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countByKeyApprox(timeout: Long, confidence: Double): PartialResult[collection.Map[K, BoundedDouble]] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countApproxDistinctByKey(p: Int, sp: Int, partitioner: Partitioner): RDD[(K, Long)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countApproxDistinctByKey(relativeSD: Double, partitioner: Partitioner): RDD[(K, Long)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countApproxDistinctByKey(relativeSD: Double, numPartitions: Int): RDD[(K, Long)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def countApproxDistinctByKey(relativeSD: Double): RDD[(K, Long)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def partitionBy(partitioner: Partitioner): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def join[W](other: rdd.RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def leftOuterJoin[W](other: rdd.RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def rightOuterJoin[W](other: rdd.RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], W))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def fullOuterJoin[W](other: rdd.RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Option[V], Option[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C,
                               mergeCombiners: (C, C) => C): RDD[(K, C)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def combineByKeyWithClassTag[C](createCombiner: V => C, mergeValue: (C, V) => C,
                                           mergeCombiners: (C, C) => C)
                                          (implicit ct: ClassTag[C]): RDD[(K, C)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupByKey(): RDD[(K, Iterable[V])] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def join[W](other: rdd.RDD[(K, W)]): RDD[(K, (V, W))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def join[W](other: rdd.RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def leftOuterJoin[W](other: rdd.RDD[(K, W)]): RDD[(K, (V, Option[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def leftOuterJoin[W](other: rdd.RDD[(K, W)], numPartitions: Int): RDD[(K, (V, Option[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def rightOuterJoin[W](other: rdd.RDD[(K, W)]): RDD[(K, (Option[V], W))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def rightOuterJoin[W](other: rdd.RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], W))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def fullOuterJoin[W](other: rdd.RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def fullOuterJoin[W](other: rdd.RDD[(K, W)], numPartitions: Int): RDD[(K, (Option[V], Option[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def collectAsMap(): collection.Map[K, V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def mapValues[U](f: V => U): RDD[(K, U)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W1, W2, W3](other1: rdd.RDD[(K, W1)], other2: rdd.RDD[(K, W2)],
                                   other3: rdd.RDD[(K, W3)], partitioner: Partitioner)
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W](other: rdd.RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W1, W2](other1: rdd.RDD[(K, W1)],
                               other2: rdd.RDD[(K, W2)],
                               partitioner: Partitioner): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W1, W2, W3](other1: rdd.RDD[(K, W1)], other2: rdd.RDD[(K, W2)],
                                   other3: rdd.RDD[(K, W3)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W](other: rdd.RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W1, W2](other1: rdd.RDD[(K, W1)], other2: rdd.RDD[(K, W2)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W](other: rdd.RDD[(K, W)], numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W1, W2](other1: rdd.RDD[(K, W1)],
                               other2: rdd.RDD[(K, W2)],
                               numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def cogroup[W1, W2, W3](other1: rdd.RDD[(K, W1)], other2: rdd.RDD[(K, W2)],
                                   other3: rdd.RDD[(K, W3)], numPartitions: Int)
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupWith[W](other: rdd.RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupWith[W1, W2](other1: rdd.RDD[(K, W1)], other2: rdd.RDD[(K, W2)])
  : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def groupWith[W1, W2, W3](other1: rdd.RDD[(K, W1)], other2: rdd.RDD[(K, W2)], other3: rdd.RDD[(K, W3)])
  : rdd.RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def subtractByKey[W](other: rdd.RDD[(K, W)])(implicit evidence$4: ClassTag[W]): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def subtractByKey[W](other: rdd.RDD[(K, W)], numPartitions: Int)
                               (implicit evidence$5: ClassTag[W]): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def subtractByKey[W](other: rdd.RDD[(K, W)], p: Partitioner)
                               (implicit evidence$6: ClassTag[W]): RDD[(K, V)] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def lookup(key: K): Seq[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String, codec: Class[_ <: CompressionCodec])
                                                        (implicit fm: ClassTag[F]): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsNewAPIHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_],
                                      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
                                      conf: Configuration): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_],
                                outputFormatClass: Class[_ <: OutputFormat[_, _]],
                                codec: Class[_ <: CompressionCodec]): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsHadoopFile(path: String, keyClass: Class[_], valueClass: Class[_],
                                outputFormatClass: Class[_ <: OutputFormat[_, _]], conf: JobConf,
                                codec: Option[Class[_ <: CompressionCodec]]): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsNewAPIHadoopDataset(conf: Configuration): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def saveAsHadoopDataset(conf: JobConf): Unit =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def keys: RDD[K] =
    throw new UnsupportedOperationException("Operation not yet implemented.")

  override def values: RDD[V] =
    throw new UnsupportedOperationException("Operation not yet implemented.")
}

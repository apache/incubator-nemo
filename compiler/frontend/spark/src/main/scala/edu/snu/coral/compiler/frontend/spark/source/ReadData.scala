package edu.snu.coral.compiler.frontend.spark.source

import java.io.FileNotFoundException

import edu.snu.coral.compiler.frontend.spark.core.java.SparkFrontendUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

object ReadData {
  def execute(sparkPlan: SparkPlan,
              partition: Partition,
              tc: TaskContext): Iterator[InternalRow] = sparkPlan match {
    //  DESERIALIZE TO OBJECT EXEC: #doExecute
    case p: DeserializeToObjectExec =>
      val childRows: Iterator[InternalRow] = this.execute(p.child, partition, tc)
      val projection = GenerateSafeProjection.generate(p.deserializer :: Nil, p.child.output)
      projection.initialize(partition.index)
      childRows.map(r => projection.apply(r))

    //  WHOLE STAGE CODE GEN EXEC: #doExecute
    case p: WholeStageCodegenExec =>
      this.execute(p.child, partition, tc)

    //  FILE SOURCE SCAN EXEC: doExecute
    case p: FileSourceScanExec =>
      val relation: HadoopFsRelation = p.relation
      val requiredSchema: StructType = p.requiredSchema
      val pushedDownFilters = p.dataFilters.flatMap(p => SparkFrontendUtils.translateFilter(p))

      val unsafeRows = {
        val scan = p.inputRDDs().head
        val rawRows: Iterator[InternalRow] =
          this.computeInputRDD(scan, partition, tc, relation, requiredSchema, pushedDownFilters)

        if (p.needsUnsafeRowConversion) {
          val attributes = p.output
          val schema = StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
          val proj = UnsafeProjection.create(schema)
          proj.initialize(partition.index)
          rawRows.map(r => proj.apply(r))
        } else {
          rawRows
        }
      }
      unsafeRows

    //  ELSE
    case p =>
      println("ELSE: " + p)
      // To catch those that are not supported
      val scan = p.asInstanceOf[DeserializeToObjectExec]
      val rdd = p.execute()
      println(" RDD: " + rdd)
      rdd.compute(partition, tc)
  }

  def computeInputRDD(rdd: RDD[InternalRow],
                      split: Partition,
                      context: TaskContext,
                      relation: HadoopFsRelation,
                      requiredSchema: StructType,
                      pushedDownFilters: Seq[Filter]): Iterator[InternalRow] = rdd match {
    case r: FileScanRDD =>
      val readFile: (PartitionedFile) => Iterator[InternalRow] =
        relation.fileFormat.buildReaderWithPartitionValues(
          sparkSession = relation.sparkSession,
          dataSchema = relation.dataSchema,
          partitionSchema = relation.partitionSchema,
          requiredSchema = requiredSchema,
          filters = pushedDownFilters,
          options = relation.options,
          hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

      val iterator = new Iterator[Object] with AutoCloseable {
        private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
        private[this] var currentFile: PartitionedFile = null
        private[this] var currentIterator: Iterator[Object] = null

        def hasNext: Boolean = {
          (currentIterator != null && currentIterator.hasNext) || nextIterator()
        }
        def next(): Object = {
          val nextElement = currentIterator.next()
          nextElement
        }

        private def readCurrentFile(): Iterator[InternalRow] = {
          try {
            readFile(currentFile)
          } catch {
            case e: FileNotFoundException =>
              throw new FileNotFoundException(
                e.getMessage + "\n" +
                  "It is possible the underlying files have been updated. " +
                  "You can explicitly invalidate the cache in Spark by " +
                  "running 'REFRESH TABLE tableName' command in SQL or " +
                  "by recreating the Dataset/DataFrame involved.")
          }
        }

        /** Advances to the next file. Returns true if a new non-empty iterator is available. */
        private def nextIterator(): Boolean = {
          if (files.hasNext) {
            currentFile = files.next()
//            InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)
            currentIterator = readCurrentFile()
            hasNext
          } else {
            currentFile = null
//            InputFileBlockHolder.unset()
            false
          }
        }

        override def close(): Unit = {
//          InputFileBlockHolder.unset()
        }
      }
      context.addTaskCompletionListener(_ => iterator.close())
      iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.

    // ELSE
    case r =>
      println("RDD ELSE: " + r)
      // To catch those that are not supported
      rdd.asInstanceOf[FileScanRDD]
      rdd.compute(split, context)
  }
}
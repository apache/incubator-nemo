package edu.snu.coral.compiler.frontend.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, GenerateSafeProjection}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{StructField, StructType}

object ReadData {
  def execute(sparkPlan: SparkPlan,
              partition: Partition,
              tc: TaskContext): Iterator[InternalRow] = sparkPlan match {
    //  DESERIALIZE TO OBJECT EXEC: #doExecute
    case p: DeserializeToObjectExec =>
      println("DESERIALIZE: " + p)
      val childRows: Iterator[InternalRow] = this.execute(p.child, partition, tc)
      val projection = GenerateSafeProjection.generate(p.deserializer :: Nil, p.child.output)
      projection.initialize(partition.index)
      childRows.map(r => projection.apply(r))

    //  WHOLE STAGE CODE GEN EXEC: #doExecute
    case p: WholeStageCodegenExec =>
      println("WHOLESTAGECODEGEN: " + p)
      val (ctx, cleanedSource) = p.doCodeGen()
      // try to compile and fallback if it failed
      try {
        CodeGenerator.compile(cleanedSource)
      } catch {
        case e: Exception =>
          return this.execute(p.child, partition, tc)
      }
      val references = ctx.references.toArray

      val durationMs = p.longMetric("pipelineTime")

      this.execute(p.child, partition, tc)

    //  FILE SOURCE SCAN EXEC: doExecute
    case p: FileSourceScanExec =>
      println("FILESOURCESCAN: " + p)
      val unsafeRows = {
        val scan = p.inputRDDs().head
        val rawRows: Iterator[InternalRow] = this.computeInputRDD(scan, partition, tc)
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
      val numOutputRows = p.longMetric("numOutputRows")
      unsafeRows.map(r => {
        numOutputRows += 1
        r
      })

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
                      context: TaskContext): Iterator[InternalRow] = {
    rdd.compute(split, context)
  }
}
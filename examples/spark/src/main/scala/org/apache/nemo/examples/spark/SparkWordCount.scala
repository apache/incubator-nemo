// scalastyle:off println
package org.apache.nemo.examples.spark

import org.apache.nemo.compiler.frontend.spark.sql.SparkSession

/**
  * Computes counts of each data key.
  */
object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkWordCount <input_file> [<output_file>]")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("Spark Word Count")
      .getOrCreate()

    val lines = spark.read().textFile(args(0)).rdd()

    val words = lines.flatMap(s => s.split(" +"))

    val ones = words.map(s => (s, 1))

    val counts = ones.reduceByKey((i1, i2) => i1 + i2)

    val parsed = counts.map(tuple => tuple._1 + ": " + tuple._2.toString)

    val writeMode = args(1) != null // write to file or print

    if (writeMode) { // print to output file
      parsed.saveAsTextFile(args(1))
    } else { // print to console.
      val output = parsed.collect()
      for (elem <- output) {
        println(elem)
      }
    }
    spark.stop()
  }
}
// scalastyle:on println

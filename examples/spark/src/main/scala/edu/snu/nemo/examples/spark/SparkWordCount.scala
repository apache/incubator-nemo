/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package edu.snu.nemo.examples.spark

import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession

/**
  * Computes counts of each data key.
  */
object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <input_file> [<output_file>]")
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

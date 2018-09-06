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
package org.apache.nemo.examples.spark

import org.apache.nemo.compiler.frontend.spark.sql.SparkSession

/**
  * Computes counts of each data key.
  */
object SparkCachingWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkWordCount <input_file> <output_file1> <output_file2>")
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

    val cached = counts.cache()

    val parsed = cached.map(tuple => tuple._1 + ": " + tuple._2.toString)

    // first collect
    val writeMode1 = args(1) != null // write to file or print
    if (writeMode1) { // print to output file
      parsed.saveAsTextFile(args(1))
    } else { // print to console.
      val output = parsed.collect()
      for (elem <- output) {
        println(elem)
      }
    }

    val reversed = cached.map(p => (p._2, p._1))

    val reversedVals = reversed.reduceByKey((string1, string2) => string1 + ", " + string2)

    val parsed2 = reversedVals.map(tuple => tuple._1 + ": " + tuple._2.toString)

    // second collect
    val writeMode2 = args(2) != null // write to file or print
    if (writeMode2) { // print to output file
      parsed2.saveAsTextFile(args(2))
    } else { // print to console.
      val output = parsed2.collect()
      for (elem <- output) {
        println(elem)
      }
    }

    spark.stop()
  }
}
// scalastyle:on println

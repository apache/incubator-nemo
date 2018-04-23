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
package edu.snu.nemo.examples.spark;

import edu.snu.nemo.compiler.frontend.spark.core.java.JavaPairRDD;
import edu.snu.nemo.compiler.frontend.spark.core.java.JavaRDD;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Java MapReduce example.
 */
public final class JavaMapReduce {

  /**
   * Private constructor.
   */
  private JavaMapReduce() {
  }

  /**
   * Main method.
   * @param args arguments.
   * @throws Exception exceptions.
   */
  public static void main(final String[] args) throws Exception {

    // Parse Arguments
    final String input = args[0];
    final String output = args[1];

    final SparkSession spark = SparkSession
        .builder()
        .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
        .appName("JavaWordCount")
        .getOrCreate();

    final long start = System.currentTimeMillis();

    // Run MR
    final JavaRDD<String> data = spark.read().textFile(input).javaRDD();
    final JavaPairRDD<String, Long> documentToCount = data
        .mapToPair(line -> {
          final String[] words = line.split(" +");
          final String documentId = words[0] + "#" + words[1];
          final long count = Long.parseLong(words[2]);
          return new Tuple2<>(documentId, count);
        });
    final JavaRDD<String> documentToSum = documentToCount
        .reduceByKey((i1, i2) -> i1 + i2)
        .map(t -> t._1() + ": " + t._2());
    documentToSum.saveAsTextFile(output);

    // DONE
    System.out.println("*******END*******");
    System.out.println("JCT(ms): " + (System.currentTimeMillis() - start));

    spark.stop();
  }
}

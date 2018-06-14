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

import edu.snu.nemo.compiler.frontend.spark.core.rdd.JavaPairRDD;
import edu.snu.nemo.compiler.frontend.spark.core.rdd.JavaRDD;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Java Wordcount example.
 */
public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  /**
   * Private constructor.
   */
  private JavaWordCount() {
  }

  /**
   * Main method.
   * @param args arguments.
   * @throws Exception exceptions.
   */
  public static void main(final String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <input_file> [<output_file>]");
      System.exit(1);
    }

    SparkSession spark = SparkSession
        .builder()
        .appName("JavaWordCount")
        .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    List<Tuple2<String, Integer>> output = counts.collect();

    final boolean writemode = args[1] != null;
    if (writemode) { // print to output file
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]))) {
        for (Tuple2<?, ?> tuple : output) {
          bw.write(tuple._1 + ": " + tuple._2 + "\n");
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else { // print to console.
      for (Tuple2<?, ?> tuple : output) {
        System.out.println(tuple._1 + ": " + tuple._2);
      }
    }
    spark.stop();
  }
}

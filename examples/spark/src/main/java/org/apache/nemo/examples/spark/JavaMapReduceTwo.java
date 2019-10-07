/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.examples.spark;

import org.apache.nemo.compiler.frontend.spark.core.JavaSparkContext;
import org.apache.nemo.compiler.frontend.spark.core.rdd.JavaPairRDD;
import org.apache.nemo.compiler.frontend.spark.core.rdd.JavaRDD;
import org.apache.nemo.compiler.frontend.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Java Wordcount example.
 */
public final class JavaMapReduceTwo {
  private static final Pattern SPACE = Pattern.compile(" ");

  /**
   * Private constructor.
   */
  private JavaMapReduceTwo() {
  }

  /**
   * Main method.
   *
   * @param args arguments.
   * @throws Exception exceptions.
   */
  public static void main(final String[] args) throws Exception {

    String file = "/home/johnyangk/input.txt";

    SparkSession ss = SparkSession
      .builder()
      .appName("JavaWordCount")
      .getOrCreate();

    final JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
    JavaRDD<String> lines = jsc.textFile(file);

    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);


    counts.saveAsTextFile("results");

    ss.stop();
  }
}

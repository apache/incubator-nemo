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
package edu.snu.coral.compiler.frontend.spark.core.java;

import org.apache.spark.SparkContext;

import java.util.List;

/**
 * Spark context wrapper for Java.
 */
public final class JavaSparkContext {
  private final SparkContext sparkContext;

  /**
   * Constructor.
   * @param sparkContext spark context to wrap.
   */
  public JavaSparkContext(final SparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  /**
   * Initiate a JavaRDD with the number of parallelism.
   * @param l input data as list.
   * @param slices number of slices (parallelism).
   * @param <T> type of the initial element.
   * @return the newly initiated JavaRDD.
   */
  public <T> JavaRDD<T> parallelize(final List<T> l, final int slices) {
    return JavaRDD.of(this.sparkContext, l, slices);
  }
}

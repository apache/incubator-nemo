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
package edu.snu.nemo.compiler.frontend.spark.sql;

import edu.snu.nemo.compiler.frontend.spark.core.java.JavaRDD;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * A dataset component: it represents relational data.
 * @param <T> type of the data.
 */
public final class Dataset<T> extends org.apache.spark.sql.Dataset<T> {
  /**
   * Constructor.
   * @param sparkSession spark session.
   * @param logicalPlan spark logical plan.
   * @param encoder spark encoder.
   */
  private Dataset(final SparkSession sparkSession, final LogicalPlan logicalPlan, final Encoder<T> encoder) {
    super(sparkSession, logicalPlan, encoder);
  }

  /**
   * Using the immutable property of datasets, we can downcast spark datasets to our class using this function.
   * @param dataset the Spark dataset.
   * @param <U> type of the dataset.
   * @return our dataset class.
   */
  public static <U> Dataset<U> from(final org.apache.spark.sql.Dataset<U> dataset) {
    return new Dataset<>((SparkSession) dataset.sparkSession(), dataset.logicalPlan(), dataset.exprEnc());
  }

  /**
   * Create a javaRDD component from this data set.
   * @return the new javaRDD component.
   */
  @Override
  public JavaRDD<T> javaRDD() {
    return JavaRDD.of((SparkSession) super.sparkSession(), this);
  }
}

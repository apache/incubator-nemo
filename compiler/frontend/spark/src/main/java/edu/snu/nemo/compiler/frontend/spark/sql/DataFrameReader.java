/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.frontend.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * A data frame reader to create the initial dataset.
 */
public final class DataFrameReader extends org.apache.spark.sql.DataFrameReader implements NemoSparkUserFacingClass {
  private final SparkSession sparkSession;

  /**
   * Constructor.
   * @param sparkSession spark session.
   */
  DataFrameReader(final SparkSession sparkSession) {
    super(sparkSession);
    this.sparkSession = sparkSession;
  }

  @Override
  public SparkSession sparkSession() {
    return this.sparkSession;
  }

  @Override
  public Dataset<Row> csv(final org.apache.spark.sql.Dataset<String> csvDataset) {
    final boolean userTriggered = initializeFunction(csvDataset);
    final Dataset<Row> result = Dataset.from(super.csv(csvDataset));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> csv(final scala.collection.Seq<String> paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.csv(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> csv(final String... paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.csv(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> csv(final String path) {
    final boolean userTriggered = initializeFunction(path);
    final Dataset<Row> result = Dataset.from(super.csv(path));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public DataFrameReader format(final String source) {
    super.format(source);
    return this;
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final java.util.Properties properties) {
    final boolean userTriggered = initializeFunction(url, table, properties);
    final Dataset<Row> result = Dataset.from(super.jdbc(url, table, properties));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table,
                           final String[] predicates, final java.util.Properties connectionProperties) {
    final boolean userTriggered = initializeFunction(url, table, predicates, connectionProperties);
    final Dataset<Row> result = Dataset.from(super.jdbc(url, table, predicates, connectionProperties));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final String columnName,
                           final long lowerBound, final long upperBound, final int numPartitions,
                           final java.util.Properties connectionProperties) {
    final boolean userTriggered = initializeFunction(
        url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties);
    final Dataset<Row> result = Dataset.from(super.jdbc(
        url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> json(final org.apache.spark.sql.Dataset<String> jsonDataset) {
    final boolean userTriggered = initializeFunction(jsonDataset);
    final Dataset<Row> result = Dataset.from(super.json(jsonDataset));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> json(final JavaRDD<String> jsonRDD) {
    final boolean userTriggered = initializeFunction(jsonRDD);
    final Dataset<Row> result = Dataset.from(super.json(jsonRDD));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> json(final RDD<String> jsonRDD) {
    final boolean userTriggered = initializeFunction(jsonRDD);
    final Dataset<Row> result = Dataset.from(super.json(jsonRDD));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> json(final scala.collection.Seq<String> paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.json(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> json(final String... paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.json(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> json(final String path) {
    final boolean userTriggered = initializeFunction(path);
    final Dataset<Row> result = Dataset.from(super.json(path));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> load() {
    final boolean userTriggered = initializeFunction();
    final Dataset<Row> result = Dataset.from(super.load());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> load(final scala.collection.Seq<String> paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.load(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> load(final String... paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.load(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> load(final String path) {
    final boolean userTriggered = initializeFunction(path);
    final Dataset<Row> result = Dataset.from(super.load(path));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public DataFrameReader option(final String key, final boolean value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(final String key, final double value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(final String key, final long value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(final String key, final String value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader options(final scala.collection.Map<String, String> options) {
    super.options(options);
    return this;
  }

  @Override
  public DataFrameReader options(final java.util.Map<String, String> options) {
    super.options(options);
    return this;
  }

  @Override
  public Dataset<Row> orc(final scala.collection.Seq<String> paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.orc(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> orc(final String... paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.orc(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> orc(final String path) {
    final boolean userTriggered = initializeFunction(path);
    final Dataset<Row> result = Dataset.from(super.orc(path));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> parquet(final scala.collection.Seq<String> paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.parquet(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> parquet(final String... paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.parquet(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> parquet(final String path) {
    final boolean userTriggered = initializeFunction(path);
    final Dataset<Row> result = Dataset.from(super.parquet(path));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public DataFrameReader schema(final StructType schema) {
    super.schema(schema);
    return this;
  }

  @Override
  public Dataset<Row> table(final String tableName) {
    final boolean userTriggered = initializeFunction(tableName);
    final Dataset<Row> result = Dataset.from(super.table(tableName));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> text(final scala.collection.Seq<String> paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.text(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> text(final String... paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<Row> result = Dataset.from(super.text(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> text(final String path) {
    final boolean userTriggered = initializeFunction(path);
    final Dataset<Row> result = Dataset.from(super.text(path));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<String> textFile(final scala.collection.Seq<String> paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<String> result = Dataset.from(super.textFile(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<String> textFile(final String... paths) {
    final boolean userTriggered = initializeFunction(paths);
    final Dataset<String> result = Dataset.from(super.textFile(paths));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<String> textFile(final String path) {
    final boolean userTriggered = initializeFunction(path);
    final Dataset<String> result = Dataset.from(super.textFile(path));
    this.setIsUserTriggered(userTriggered);
    return result;
  }
}

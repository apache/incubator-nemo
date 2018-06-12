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

import edu.snu.nemo.compiler.frontend.spark.core.rdd.JavaRDD;
import edu.snu.nemo.compiler.frontend.spark.core.rdd.RDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.storage.StorageLevel;

import java.util.stream.Stream;

/**
 * A dataset component: it represents relational data.
 * @param <T> type of the data.
 */
public final class Dataset<T> extends org.apache.spark.sql.Dataset<T> implements NemoSparkUserFacingClass {
  /**
   * Constructor.
   *
   * @param sparkSession spark session.
   * @param logicalPlan  spark logical plan.
   * @param encoder      spark encoder.
   */
  private Dataset(final SparkSession sparkSession, final LogicalPlan logicalPlan, final Encoder<T> encoder) {
    super(sparkSession, logicalPlan, encoder);
  }

  /**
   * Using the immutable property of datasets, we can downcast spark datasets to our class using this function.
   *
   * @param dataset the Spark dataset.
   * @param <U>     type of the dataset.
   * @return our dataset class.
   */
  public static <U> Dataset<U> from(final org.apache.spark.sql.Dataset<U> dataset) {
    if (dataset instanceof Dataset) {
      return (Dataset<U>) dataset;
    } else {
      return new Dataset<>((SparkSession) dataset.sparkSession(), dataset.logicalPlan(), dataset.exprEnc());
    }
  }

  /**
   * Create a javaRDD component from this data set.
   *
   * @return the new javaRDD component.
   */
  @Override
  public JavaRDD<T> javaRDD() {
    return this.toJavaRDD();
  }

  @Override
  public JavaRDD<T> toJavaRDD() {
    return JavaRDD.of((SparkSession) super.sparkSession(), this);
  }

  /**
   * Create a actual {@link RDD} component of Spark to get the source data.
   * This method should not be called by any user program.
   *
   * @return a Spark RDD from this dataset.
   */
  public org.apache.spark.rdd.RDD<T> sparkRDD() {
    return super.rdd();
  }

  /**
   * Create a {@link RDD} component from this data set.
   * To transparently give our RDD to user programs, this method have to be overridden.
   *
   * By overriding this method, if a method (such as reduce) of super ({@link org.apache.spark.sql.Dataset}) is called
   * and it uses super's rdd, the rdd will be our rdd returned by this method.
   * This is an intended behavior and the result will be calculated by our system.
   *
   * @return the new RDD component.
   */
   @Override
   public RDD<T> rdd() {
     final JavaRDD<T> javaRDD = JavaRDD.of((SparkSession) super.sparkSession(), this);
     return javaRDD.rdd();
   }

  @Override
  public Dataset<Row> agg(final Column expr, final Column... exprs) {
    final boolean userTriggered = initializeFunction(expr, exprs);
    final Dataset<Row> result = from(super.agg(expr, exprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> agg(final Column expr, final scala.collection.Seq<Column> exprs) {
    final boolean userTriggered = initializeFunction(expr, exprs);
    final Dataset<Row> result = from(super.agg(expr, exprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> agg(final scala.collection.immutable.Map<String, String> exprs) {
    final boolean userTriggered = initializeFunction(exprs);
    final Dataset<Row> result = from(super.agg(exprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> agg(final java.util.Map<String, String> exprs) {
    final boolean userTriggered = initializeFunction(exprs);
    final Dataset<Row> result = from(super.agg(exprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> agg(final scala.Tuple2<String, String> aggExpr,
                          final scala.collection.Seq<scala.Tuple2<String, String>> aggExprs) {
    final boolean userTriggered = initializeFunction(aggExpr, aggExprs);
    final Dataset<Row> result = from(super.agg(aggExpr, aggExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> alias(final String alias) {
    final boolean userTriggered = initializeFunction(alias);
    final Dataset<T> result = from(super.alias(alias));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> alias(final scala.Symbol alias) {
    final boolean userTriggered = initializeFunction(alias);
    final Dataset<T> result = from(super.alias(alias));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <U> Dataset<U> as(final Encoder<U> evidence) {
    final boolean userTriggered = initializeFunction(evidence);
    final Dataset<U> result = from(super.as(evidence));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> as(final String alias) {
    final boolean userTriggered = initializeFunction(alias);
    final Dataset<T> result = from(super.as(alias));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> as(final scala.Symbol alias) {
    final boolean userTriggered = initializeFunction(alias);
    final Dataset<T> result = from(super.as(alias));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> cache() {
    final boolean userTriggered = initializeFunction();
    final Dataset<T> result = from(super.cache());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> checkpoint() {
    final boolean userTriggered = initializeFunction();
    final Dataset<T> result = from(super.checkpoint());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> checkpoint(final boolean eager) {
    final boolean userTriggered = initializeFunction(eager);
    final Dataset<T> result = from(super.checkpoint(eager));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> coalesce(final int numPartitions) {
    final boolean userTriggered = initializeFunction(numPartitions);
    final Dataset<T> result = from(super.coalesce(numPartitions));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> crossJoin(final org.apache.spark.sql.Dataset<?> right) {
    final boolean userTriggered = initializeFunction(right);
    final Dataset<Row> result = from(super.crossJoin(right));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> describe(final scala.collection.Seq<String> cols) {
    final boolean userTriggered = initializeFunction(cols);
    final Dataset<Row> result = from(super.describe(cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> describe(final String... cols) {
    final boolean userTriggered = initializeFunction(cols);
    final Dataset<Row> result = from(super.describe(cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> distinct() {
    final boolean userTriggered = initializeFunction();
    final Dataset<T> result = from(super.distinct());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> drop(final Column col) {
    final boolean userTriggered = initializeFunction(col);
    final Dataset<Row> result = from(super.drop(col));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> drop(final scala.collection.Seq<String> colNames) {
    final boolean userTriggered = initializeFunction(colNames);
    final Dataset<Row> result = from(super.drop(colNames));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> drop(final String... colNames) {
    final boolean userTriggered = initializeFunction(colNames);
    final Dataset<Row> result = from(super.drop(colNames));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> drop(final String colName) {
    final boolean userTriggered = initializeFunction(colName);
    final Dataset<Row> result = from(super.drop(colName));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> dropDuplicates() {
    final boolean userTriggered = initializeFunction();
    final Dataset<T> result = from(super.dropDuplicates());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> dropDuplicates(final scala.collection.Seq<String> colNames) {
    final boolean userTriggered = initializeFunction(colNames);
    final Dataset<T> result = from(super.dropDuplicates(colNames));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> dropDuplicates(final String[] colNames) {
    final boolean userTriggered = initializeFunction(colNames);
    final Dataset<T> result = from(super.dropDuplicates(colNames));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final scala.collection.Seq<String> cols) {
    final boolean userTriggered = initializeFunction(col1, cols);
    final Dataset<T> result = from(super.dropDuplicates(col1, cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final String... cols) {
    final boolean userTriggered = initializeFunction(col1, cols);
    final Dataset<T> result = from(super.dropDuplicates(col1, cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> except(final org.apache.spark.sql.Dataset<T> other) {
    final boolean userTriggered = initializeFunction(other);
    final Dataset<T> result = from(super.except(other));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> filter(final Column condition) {
    final boolean userTriggered = initializeFunction(condition);
    final Dataset<T> result = from(super.filter(condition));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> filter(final String conditionExpr) {
    final boolean userTriggered = initializeFunction(conditionExpr);
    final Dataset<T> result = from(super.filter(conditionExpr));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> hint(final String name, final Object... parameters) {
    final boolean userTriggered = initializeFunction(name, parameters);
    final Dataset<T> result = from(super.hint(name, parameters));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> hint(final String name, final scala.collection.Seq<Object> parameters) {
    final boolean userTriggered = initializeFunction(name, parameters);
    final Dataset<T> result = from(super.hint(name, parameters));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> intersect(final org.apache.spark.sql.Dataset<T> other) {
    final boolean userTriggered = initializeFunction(other);
    final Dataset<T> result = from(super.intersect(other));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right) {
    final boolean userTriggered = initializeFunction(right);
    final Dataset<Row> result = from(super.join(right));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs) {
    final boolean userTriggered = initializeFunction(right, joinExprs);
    final Dataset<Row> result = from(super.join(right, joinExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs, final String joinType) {
    final boolean userTriggered = initializeFunction(right, joinExprs, joinType);
    final Dataset<Row> result = from(super.join(right, joinExprs, joinType));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right,
                           final scala.collection.Seq<String> usingColumns) {
    final boolean userTriggered = initializeFunction(right, usingColumns);
    final Dataset<Row> result = from(super.join(right, usingColumns));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final scala.collection.Seq<String> usingColumns,
                           final String joinType) {
    final boolean userTriggered = initializeFunction(right, usingColumns, joinType);
    final Dataset<Row> result = from(super.join(right, usingColumns, joinType));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final String usingColumn) {
    final boolean userTriggered = initializeFunction(right, usingColumn);
    final Dataset<Row> result = from(super.join(right, usingColumn));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> limit(final int n) {
    final boolean userTriggered = initializeFunction(n);
    final Dataset<T> result = from(super.limit(n));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <U> Dataset<U> map(final scala.Function1<T, U> func, final Encoder<U> evidence) {
    final boolean userTriggered = initializeFunction(func, evidence);
    final Dataset<U> result = from(super.map(func, evidence));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <U> Dataset<U> map(final MapFunction<T, U> func, final Encoder<U> encoder) {
    final boolean userTriggered = initializeFunction(func, encoder);
    final Dataset<U> result = from(super.map(func, encoder));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <U> Dataset<U> mapPartitions(
      final scala.Function1<scala.collection.Iterator<T>, scala.collection.Iterator<U>> func,
      final Encoder<U> evidence) {
    final boolean userTriggered = initializeFunction(func, evidence);
    final Dataset<U> result = from(super.mapPartitions(func, evidence));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <U> Dataset<U> mapPartitions(final MapPartitionsFunction<T, U> f, final Encoder<U> encoder) {
    final boolean userTriggered = initializeFunction(f, encoder);
    final Dataset<U> result = from(super.mapPartitions(f, encoder));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  /**
   * Overrides super.ofRows.
   *
   * @param sparkSession Spark Session.
   * @param logicalPlan  Spark logical plan.
   * @return Dataset of the given rows.
   */
  public static Dataset<Row> ofRows(final org.apache.spark.sql.SparkSession sparkSession,
                                    final org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan) {
    return from(org.apache.spark.sql.Dataset.ofRows(sparkSession, logicalPlan));
  }

  @Override
  public Dataset<T> orderBy(final Column... sortExprs) {
    final boolean userTriggered = initializeFunction(sortExprs);
    final Dataset<T> result = from(super.orderBy(sortExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> orderBy(final scala.collection.Seq<Column> sortExprs) {
    final boolean userTriggered = initializeFunction(sortExprs);
    final Dataset<T> result = from(super.orderBy(sortExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final scala.collection.Seq<String> sortCols) {
    final boolean userTriggered = initializeFunction(sortCol, sortCols);
    final Dataset<T> result = from(super.orderBy(sortCol, sortCols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final String... sortCols) {
    final boolean userTriggered = initializeFunction(sortCol, sortCols);
    final Dataset<T> result = from(super.orderBy(sortCol, sortCols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> persist() {
    final boolean userTriggered = initializeFunction();
    final Dataset<T> result = from(super.persist());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> persist(final StorageLevel newLevel) {
    final boolean userTriggered = initializeFunction(newLevel);
    final Dataset<T> result = from(super.persist(newLevel));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T>[] randomSplit(final double[] weights) {
    final boolean userTriggered = initializeFunction(weights);
    final Dataset<T>[] result = Stream.of(super.randomSplit(weights)).map(ds -> from(ds)).toArray(Dataset[]::new);
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T>[] randomSplit(final double[] weights, final long seed) {
    final boolean userTriggered = initializeFunction(weights, seed);
    final Dataset<T>[] result = Stream.of(super.randomSplit(weights, seed)).map(ds -> from(ds)).toArray(Dataset[]::new);
    this.setIsUserTriggered(userTriggered);
    return result;
  }

//  @Override
//  public java.util.List<Dataset<T>> randomSplitAsList(double[] weights, long seed) {
//    return super.randomSplitAsList(weights, seed).stream().map(ds -> from(ds)).collect(Collectors.toList());
//  }

  @Override
  public Dataset<T> repartition(final Column... partitionExprs) {
    final boolean userTriggered = initializeFunction(partitionExprs);
    final Dataset<T> result = from(super.repartition(partitionExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> repartition(final int numPartitions) {
    final boolean userTriggered = initializeFunction(numPartitions);
    final Dataset<T> result = from(super.repartition(numPartitions));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final Column... partitionExprs) {
    final boolean userTriggered = initializeFunction(numPartitions, partitionExprs);
    final Dataset<T> result = from(super.repartition(numPartitions, partitionExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final scala.collection.Seq<Column> partitionExprs) {
    final boolean userTriggered = initializeFunction(numPartitions, partitionExprs);
    final Dataset<T> result = from(super.repartition(numPartitions, partitionExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> repartition(final scala.collection.Seq<Column> partitionExprs) {
    final boolean userTriggered = initializeFunction(partitionExprs);
    final Dataset<T> result = from(super.repartition(partitionExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction) {
    final boolean userTriggered = initializeFunction(withReplacement, fraction);
    final Dataset<T> result = from(super.sample(withReplacement, fraction));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction, final long seed) {
    final boolean userTriggered = initializeFunction(withReplacement, fraction, seed);
    final Dataset<T> result = from(super.sample(withReplacement, fraction, seed));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> select(final Column... cols) {
    final boolean userTriggered = initializeFunction(cols);
    final Dataset<Row> result = from(super.select(cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> select(final scala.collection.Seq<Column> cols) {
    final boolean userTriggered = initializeFunction(cols);
    final Dataset<Row> result = from(super.select(cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> select(final String col, final scala.collection.Seq<String> cols) {
    final boolean userTriggered = initializeFunction(col, cols);
    final Dataset<Row> result = from(super.select(col, cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> select(final String col, final String... cols) {
    final boolean userTriggered = initializeFunction(col, cols);
    final Dataset<Row> result = from(super.select(col, cols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <U1> Dataset<U1> select(final TypedColumn<T, U1> c1) {
    final boolean userTriggered = initializeFunction(c1);
    final Dataset<U1> result = from(super.select(c1));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> selectExpr(final scala.collection.Seq<String> exprs) {
    final boolean userTriggered = initializeFunction(exprs);
    final Dataset<Row> result = from(super.selectExpr(exprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> selectExpr(final String... exprs) {
    final boolean userTriggered = initializeFunction(exprs);
    final Dataset<Row> result = from(super.selectExpr(exprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sort(final Column... sortExprs) {
    final boolean userTriggered = initializeFunction(sortExprs);
    final Dataset<T> result = from(super.sort(sortExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sort(final scala.collection.Seq<Column> sortExprs) {
    final boolean userTriggered = initializeFunction(sortExprs);
    final Dataset<T> result = from(super.sort(sortExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sort(final String sortCol, final scala.collection.Seq<String> sortCols) {
    final boolean userTriggered = initializeFunction(sortCol, sortCols);
    final Dataset<T> result = from(super.sort(sortCol, sortCols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sort(final String sortCol, final String... sortCols) {
    final boolean userTriggered = initializeFunction(sortCol, sortCols);
    final Dataset<T> result = from(super.sort(sortCol, sortCols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sortWithinPartitions(final Column... sortExprs) {
    final boolean userTriggered = initializeFunction(sortExprs);
    final Dataset<T> result = from(super.sortWithinPartitions(sortExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sortWithinPartitions(final scala.collection.Seq<Column> sortExprs) {
    final boolean userTriggered = initializeFunction(sortExprs);
    final Dataset<T> result = from(super.sortWithinPartitions(sortExprs));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final scala.collection.Seq<String> sortCols) {
    final boolean userTriggered = initializeFunction(sortCol, sortCols);
    final Dataset<T> result = from(super.sortWithinPartitions(sortCol, sortCols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final String... sortCols) {
    final boolean userTriggered = initializeFunction(sortCol, sortCols);
    final Dataset<T> result = from(super.sortWithinPartitions(sortCol, sortCols));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public SparkSession sparkSession() {
    return (SparkSession) super.sparkSession();
  }

  @Override
  public Dataset<Row> toDF() {
    final boolean userTriggered = initializeFunction();
    final Dataset<Row> result = from(super.toDF());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> toDF(final scala.collection.Seq<String> colNames) {
    final boolean userTriggered = initializeFunction(colNames);
    final Dataset<Row> result = from(super.toDF(colNames));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> toDF(final String... colNames) {
    final boolean userTriggered = initializeFunction(colNames);
    final Dataset<Row> result = from(super.toDF(colNames));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<String> toJSON() {
    final boolean userTriggered = initializeFunction();
    final Dataset<String> result = from(super.toJSON());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <U> Dataset<U> transform(
      final scala.Function1<org.apache.spark.sql.Dataset<T>, org.apache.spark.sql.Dataset<U>> t) {
    final boolean userTriggered = initializeFunction(t);
    final Dataset<U> result = from(super.transform(t));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> union(final org.apache.spark.sql.Dataset<T> other) {
    final boolean userTriggered = initializeFunction(other);
    final Dataset<T> result = from(super.union(other));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> unpersist() {
    final boolean userTriggered = initializeFunction();
    final Dataset<T> result = from(super.unpersist());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> unpersist(final boolean blocking) {
    final boolean userTriggered = initializeFunction(blocking);
    final Dataset<T> result = from(super.unpersist(blocking));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> where(final Column condition) {
    final boolean userTriggered = initializeFunction(condition);
    final Dataset<T> result = from(super.where(condition));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<T> where(final String conditionExpr) {
    final boolean userTriggered = initializeFunction(conditionExpr);
    final Dataset<T> result = from(super.where(conditionExpr));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> withColumn(final String colName, final Column col) {
    final boolean userTriggered = initializeFunction(colName, col);
    final Dataset<Row> result = from(super.withColumn(colName, col));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> withColumnRenamed(final String existingName, final String newName) {
    final boolean userTriggered = initializeFunction(existingName, newName);
    final Dataset<Row> result = from(super.withColumnRenamed(existingName, newName));
    this.setIsUserTriggered(userTriggered);
    return result;
  }
}

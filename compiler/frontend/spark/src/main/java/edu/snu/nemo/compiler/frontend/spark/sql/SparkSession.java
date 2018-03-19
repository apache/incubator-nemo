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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import javax.naming.OperationNotSupportedException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * A simple version of the Spark session, containing SparkContext that contains SparkConf.
 */
public final class SparkSession extends org.apache.spark.sql.SparkSession implements NemoSparkUserFacingClass {
  private final LinkedHashMap<String, Object[]> datasetCommandsList;
  private final Map<String, String> initialConf;
  private final AtomicBoolean isUserTriggered;

  /**
   * Constructor.
   *
   * @param sparkContext the spark context for the session.
   * @param initialConf initial spark session configuration.
   */
  private SparkSession(final SparkContext sparkContext, final Map<String, String> initialConf) {
    super(sparkContext);
    this.datasetCommandsList = new LinkedHashMap<>();
    this.initialConf = initialConf;
    this.isUserTriggered = new AtomicBoolean(true);
  }

  @Override
  public boolean getIsUserTriggered() {
    return isUserTriggered.get();
  }

  @Override
  public void setIsUserTriggered(final boolean isUserTriggered) {
    this.isUserTriggered.set(isUserTriggered);
  }

  @Override
  public SparkSession sparkSession() {
    return this;
  }

  /**
   * Append the command to the list of dataset commands.
   *
   * @param cmd  the name of the command to apply. e.g. "SparkSession#read"
   * @param args arguments required for the command.
   */
  void appendCommand(final String cmd, final Object... args) {
    this.datasetCommandsList.put(cmd, args);
  }

  /**
   * @return the commands list required to recreate the dataset on separate machines.
   */
  public LinkedHashMap<String, Object[]> getDatasetCommandsList() {
    return this.datasetCommandsList;
  }

  /**
   * @return the initial configuration of the session.
   */
  public Map<String, String> getInitialConf() {
    return initialConf;
  }

  /**
   * Method to reproduce the initial Dataset on separate evaluators when reading from sources.
   *
   * @param spark       sparkSession to start from.
   * @param commandList commands required to setup the dataset.
   * @param <T>         type of the resulting dataset's data.
   * @return the initialized dataset.
   * @throws OperationNotSupportedException exception when the command is not yet supported.
   */
  public static <T> Dataset<T> initializeDataset(final SparkSession spark,
                                                 final LinkedHashMap<String, Object[]> commandList)
      throws OperationNotSupportedException {
    Object result = spark;

    for (Map.Entry<String, Object[]> command : commandList.entrySet()) {
      final String[] cmd = command.getKey().split("#");
      final String className = cmd[0];
      final String methodName = cmd[1];
      final Object[] args = command.getValue();
      final Class<?>[] argTypes = Stream.of(args).map(Object::getClass).toArray(Class[]::new);

      if (!SparkSession.class.getName().equals(className)
          && !DataFrameReader.class.getName().equals(className)
          && !Dataset.class.getName().equals(className)) {
        throw new OperationNotSupportedException(command + " is not yet supported.");
      }

      try {
        final Method method = result.getClass().getDeclaredMethod(methodName, argTypes);
        result = method.invoke(result, args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return (Dataset<T>) result;
  }

  @Override
  public DataFrameReader read() {
    final boolean userTriggered = initializeFunction();
    final DataFrameReader result = new DataFrameReader(this);
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> baseRelationToDataFrame(final BaseRelation baseRelation) {
    final boolean userTriggered = initializeFunction(baseRelation);
    final Dataset<Row> result = Dataset.from(super.baseRelationToDataFrame(baseRelation));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> createDataFrame(final JavaRDD<?> rdd, final Class<?> beanClass) {
    final boolean userTriggered = initializeFunction(rdd, beanClass);
    final Dataset<Row> result = Dataset.from(super.createDataFrame(rdd, beanClass));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> createDataFrame(final JavaRDD<Row> rowRDD, final StructType schema) {
    final boolean userTriggered = initializeFunction(rowRDD, schema);
    final Dataset<Row> result = Dataset.from(super.createDataFrame(rowRDD, schema));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> createDataFrame(final java.util.List<?> data, final Class<?> beanClass) {
    final boolean userTriggered = initializeFunction(data, beanClass);
    final Dataset<Row> result = Dataset.from(super.createDataFrame(data, beanClass));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> createDataFrame(final java.util.List<Row> rows, final StructType schema) {
    final boolean userTriggered = initializeFunction(rows, schema);
    final Dataset<Row> result = Dataset.from(super.createDataFrame(rows, schema));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> createDataFrame(final RDD<?> rdd, final Class<?> beanClass) {
    final boolean userTriggered = initializeFunction(rdd, beanClass);
    final Dataset<Row> result = Dataset.from(super.createDataFrame(rdd, beanClass));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> createDataFrame(final RDD<Row> rowRDD, final StructType schema) {
    final boolean userTriggered = initializeFunction(rowRDD, schema);
    final Dataset<Row> result = Dataset.from(super.createDataFrame(rowRDD, schema));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <T> Dataset<T> createDataset(final java.util.List<T> data, final Encoder<T> evidence) {
    final boolean userTriggered = initializeFunction(data, evidence);
    final Dataset<T> result = Dataset.from(super.createDataset(data, evidence));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <T> Dataset<T> createDataset(final RDD<T> data, final Encoder<T> evidence) {
    final boolean userTriggered = initializeFunction(data, evidence);
    final Dataset<T> result = Dataset.from(super.createDataset(data, evidence));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public <T> Dataset<T> createDataset(final scala.collection.Seq<T> data, final Encoder<T> evidence) {
    final boolean userTriggered = initializeFunction(data, evidence);
    final Dataset<T> result = Dataset.from(super.createDataset(data, evidence));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> emptyDataFrame() {
    final boolean userTriggered = initializeFunction();
    final Dataset<Row> result = Dataset.from(super.emptyDataFrame());
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> sql(final String sqlText) {
    final boolean userTriggered = initializeFunction(sqlText);
    final Dataset<Row> result = Dataset.from(super.sql(sqlText));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  @Override
  public Dataset<Row> table(final String tableName) {
    final boolean userTriggered = initializeFunction(tableName);
    final Dataset<Row> result = Dataset.from(super.table(tableName));
    this.setIsUserTriggered(userTriggered);
    return result;
  }

  /**
   * Method to downcast Spark's spark session to our spark session class.
   *
   * @param sparkSession spark's spark session.
   * @param initialConf  initial configuration of the spark session.
   * @return our spark session class.
   */
  public static SparkSession from(final org.apache.spark.sql.SparkSession sparkSession,
                                  final Map<String, String> initialConf) {
    return new SparkSession(sparkSession.sparkContext(), initialConf);
  }

  /**
   * Get a builder for the session.
   *
   * @return the session builder.
   */
  public static Builder builder() {
    return new Builder().master("local");
  }

  /**
   * Spark Session Builder.
   */
  public static final class Builder extends org.apache.spark.sql.SparkSession.Builder {
    private final Map<String, String> options = new HashMap<>();

    @Override
    public Builder appName(final String name) {
      return (Builder) super.appName(name);
    }

    @Override
    public Builder config(final SparkConf conf) {
      for (Tuple2<String, String> kv : conf.getAll()) {
        this.options.put(kv._1, kv._2);
      }
      return (Builder) super.config(conf);
    }

    /**
     * Apply config in the form of Java Map.
     *
     * @param conf the conf.
     * @return the builder with the conf applied.
     */
    public Builder config(final Map<String, String> conf) {
      conf.forEach((k, v) -> {
        this.options.put(k, v);
        super.config(k, v);
      });
      return this;
    }

    @Override
    public Builder config(final String key, final String value) {
      this.options.put(key, value);
      return (Builder) super.config(key, value);
    }

    @Override
    public Builder master(final String master) {
      return (Builder) super.master(master);
    }

    @Override
    public SparkSession getOrCreate() {
      UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("nemo_user"));
      return SparkSession.from(super.getOrCreate(), this.options);
    }
  }
}

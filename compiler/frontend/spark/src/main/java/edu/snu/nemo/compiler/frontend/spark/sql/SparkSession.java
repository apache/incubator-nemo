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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.Tuple2;

import javax.naming.OperationNotSupportedException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple version of the Spark session, containing SparkContext that contains SparkConf.
 */
public final class SparkSession extends org.apache.spark.sql.SparkSession {
  private final LinkedHashMap<String, Object[]> datasetCommandsList;
  private final Map<String, String> initialConf;

  /**
   * Constructor.
   * @param sparkContext the spark context for the session.
   */
  private SparkSession(final SparkContext sparkContext, final Map<String, String> initialConf) {
    super(sparkContext);
    this.datasetCommandsList = new LinkedHashMap<>();
    this.initialConf = initialConf;
  }

  /**
   * Append the command to the list of dataset commands.
   * @param cmd the name of the command to apply. e.g. "SparkSession#read"
   * @param args arguments required for the command.
   */
  public void appendCommand(final String cmd, final Object... args) {
    this.datasetCommandsList.put(cmd, args);
  }

  /**
   * @return the commands list required to recreate the dataset on separate machines.
   */
  public LinkedHashMap<String, Object[]> getDatasetCommandsList() {
    return datasetCommandsList;
  }

  /**
   * @return the initial configuration of the session.
   */
  public Map<String, String> getInitialConf() {
    return initialConf;
  }

  /**
   * Method to reproduce the initial Dataset on separate evaluators when reading from sources.
   * @param spark sparkSession to start from.
   * @param commandList commands required to setup the dataset.
   * @param <T> type of the resulting dataset's data.
   * @return the initialized dataset.
   * @throws OperationNotSupportedException exception when the command is not yet supported.
   */
  public static <T> Dataset<T> initializeDataset(final SparkSession spark,
                                                 final LinkedHashMap<String, Object[]> commandList)
      throws OperationNotSupportedException {
    Object result = spark;

    for (Map.Entry<String, Object[]> command: commandList.entrySet()) {
      final String cmd = command.getKey();
      final Object[] args = command.getValue();

      //TODO#776: support more commands related to initialization of dataset.
      switch (cmd) {
        case "SparkSession#read":
          result = ((SparkSession) result).read();
          break;
        case "DataFrameReader#textFile":
          result = ((DataFrameReader) result).textFile((String) args[0]);
          break;
        default:
          throw new OperationNotSupportedException(cmd + " is not yet supported.");
      }
    }

    return (Dataset<T>) result;
  }

  @Override
  public DataFrameReader read() {
    appendCommand("SparkSession#read");
    return new DataFrameReader(this);
  }

  /**
   * Method to downcast Spark's spark session to our spark session class.
   * @param sparkSession spark's spark session.
   * @param initialConf initial configuration of the spark session.
   * @return our spark session class.
   */
  public static SparkSession from(final org.apache.spark.sql.SparkSession sparkSession,
                                  final Map<String, String> initialConf) {
    return new SparkSession(sparkSession.sparkContext(), initialConf);
  }

  /**
   * Get a builder for the session.
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

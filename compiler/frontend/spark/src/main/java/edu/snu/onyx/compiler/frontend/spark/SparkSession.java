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
package edu.snu.onyx.compiler.frontend.spark;

import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.UUID;

/**
 * A simple version of the Spark session, containing SparkContext that contains SparkConf.
 */
public final class SparkSession {
  private final HashMap<String, String> initialSessionOptions;
  private final SparkContext sparkContext;

  /**
   * Constructor.
   * @param sparkContext the spark context for the session.
   */
  private SparkSession(final SparkContext sparkContext) {
    this.initialSessionOptions = new HashMap<>();
    this.sparkContext = sparkContext;
  }

  /**
   * Get the spark context.
   * @return the spark context of the session.
   */
  public SparkContext sparkContext() {
    return sparkContext;
  }

  /**
   * Get a builder for the session.
   * @return the session builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * stop the session.
   */
  public void stop() {
  }

  /**
   * Spark Session Builder.
   */
  public static final class Builder {
    private final HashMap<String, String> options;

    /**
     * Default constructor.
     */
    private Builder() {
      this.options = new HashMap<>();
    }

    /**
     * set the application name of the session.
     * @param name the name of the session.
     * @return the builder.
     */
    public Builder appName(final String name) {
      return config("spark.app.name", name);
    }

    /**
     * Set a configuration to the session.
     * @param key key of the configuration.
     * @param value value of the configuration.
     * @return the builder with the configuration set.
     */
    public Builder config(final String key, final String value) {
      this.options.put(key, value);
      return this;
    }

    /**
     * Get or create the new Spark Session.
     * @return the Spark Session.
     */
    public SparkSession getOrCreate() {
      final SparkConf sparkConf = new SparkConf();
      final SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
      options.forEach(sparkContext.conf()::set);
      if (!options.containsKey("spark.app.name")) {
        sparkContext.conf().setAppName("GeneratedAppName-" + UUID.randomUUID());
      }

      final SparkSession session = new SparkSession(sparkContext);
      options.forEach(session.initialSessionOptions::put);

      return session;
    }
  }
}

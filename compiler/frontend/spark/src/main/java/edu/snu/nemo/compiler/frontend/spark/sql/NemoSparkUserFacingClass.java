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

/**
 * Common interface for SparkSQL supporting Nemo classes containing the methods that are exposed to users.
 * We currently support {@link SparkSession}, {@link Dataset}, {@link DataFrameReader} APIs.
 */
public interface NemoSparkUserFacingClass {
  /**
   * @return the userTriggered flag.
   */
  default boolean getIsUserTriggered() {
    return sparkSession().getIsUserTriggered();
  }

  /**
   * Set the userTriggered flag.
   * @param bool boolean to set the flag to.
   */
  default void setIsUserTriggered(boolean bool) {
    sparkSession().setIsUserTriggered(bool);
  }

  /**
   * @return the sparkSession of the instance.
   */
  SparkSession sparkSession();

  /**
   * A method to distinguish user-called functions from internal calls.
   * @param args arguments of the method
   * @return whether or not this function has been called by the user.
   */
  default boolean initializeFunction(final Object... args) {
    if (!getIsUserTriggered()) {
      return false;
    }

    final String className = Thread.currentThread().getStackTrace()[2].getClassName();
    final String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();

    sparkSession().appendCommand(className + "#" + methodName, args);
    setIsUserTriggered(false);

    return true;
  }
}

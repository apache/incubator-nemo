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

package edu.snu.nemo.examples.spark.sql;

import edu.snu.nemo.compiler.frontend.spark.sql.Dataset;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * Java SparkSQL example: User-defined Typed Aggregation.
 *
 * This code has been copied from the Apache Spark (https://github.com/apache/spark) to demonstrate a spark example.
 */
public final class JavaUserDefinedTypedAggregation {

  /**
   * Private constructor.
   */
  private JavaUserDefinedTypedAggregation() {
  }

  /**
   * Employee class.
   */
  public static final class Employee implements Serializable {
    private String name;
    private long salary;

    /**
     * Getter.
     * @return name.
     */
    public String getName() {
      return name;
    }

    /**
     * Setter.
     * @param name name.
     */
    public void setName(final String name) {
      this.name = name;
    }

    /**
     * Getter.
     * @return salary.
     */
    public long getSalary() {
      return salary;
    }

    /**
     * Setter.
     * @param salary salary.
     */
    public void setSalary(final long salary) {
      this.salary = salary;
    }
  }

  /**
   * Average class.
   */
  public static final class Average implements Serializable  {
    private long sum;
    private long count;


    /**
     * Default constructor.
     */
    public Average() {
    }

    /**
     * Public constructor.
     * @param sum sum.
     * @param count count.
     */
    public Average(final long sum, final long count) {
      this.sum = sum;
      this.count = count;
    }

    /**
     * Getter.
     * @return sum.
     */
    public long getSum() {
      return sum;
    }

    /**
     * Setter.
     * @param sum sum.
     */
    public void setSum(final long sum) {
      this.sum = sum;
    }

    /**
     * Getter.
     * @return count.
     */
    public long getCount() {
      return count;
    }

    /**
     * Setter.
     * @param count count.
     */
    public void setCount(final long count) {
      this.count = count;
    }
  }

  /**
   * MyAverage class.
   */
  public static final class MyAverage extends Aggregator<Employee, Average, Double> {

    /**
     * A zero value for this aggregation. Should satisfy the property that any b + zero = b.
     *
     * @return zero.
     */
    public Average zero() {
      return new Average(0L, 0L);
    }


    /**
     * Combine two values to produce a new value.
     * For performance, the function may modify `buffer` and return it instead of constructing a new object.
     *
     * @param buffer first value.
     * @param employee second value.
     * @return average.
     */
    public Average reduce(final Average buffer, final Employee employee) {
      long newSum = buffer.getSum() + employee.getSalary();
      long newCount = buffer.getCount() + 1;
      buffer.setSum(newSum);
      buffer.setCount(newCount);
      return buffer;
    }

    /**
     * Merge two intermediate values.
     *
     * @param b1 first value.
     * @param b2 second value.
     * @return merged result.
     */
    public Average merge(final Average b1, final Average b2) {
      long mergedSum = b1.getSum() + b2.getSum();
      long mergedCount = b1.getCount() + b2.getCount();
      b1.setSum(mergedSum);
      b1.setCount(mergedCount);
      return b1;
    }

    /**
     * Transform the output of the reduction.
     *
     * @param reduction reduction to transform.
     * @return the transformed result.
     */
    public Double finish(final Average reduction) {
      return ((double) reduction.getSum()) / reduction.getCount();
    }

    /**
     * Specifies the EncoderFactory for the intermediate value type.
     *
     * @return buffer encoder.
     */
    public Encoder<Average> bufferEncoder() {
      return Encoders.bean(Average.class);
    }

    /**
     * Specifies the EncoderFactory for the final output value type.
     *
     * @return output encoder.
     */
    public Encoder<Double> outputEncoder() {
      return Encoders.DOUBLE();
    }
  }

  /**
   * Main function.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL user-defined Datasets aggregation example")
        .getOrCreate();

    Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
    String path = args[0];
    Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
    ds.show();
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    MyAverage myAverage = new MyAverage();
    // Convert the function to a `TypedColumn` and give it a name
    TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
    Dataset<Double> result = ds.select(averageSalary);
    result.show();
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
    spark.stop();
  }

}

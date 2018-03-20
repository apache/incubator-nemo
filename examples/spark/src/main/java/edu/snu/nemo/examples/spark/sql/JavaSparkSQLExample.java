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

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import edu.snu.nemo.compiler.frontend.spark.core.java.JavaRDD;
import edu.snu.nemo.compiler.frontend.spark.sql.Dataset;
import edu.snu.nemo.compiler.frontend.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;

/**
 * Java Spark SQL Example program.
 *
 * This code has been copied from the Apache Spark (https://github.com/apache/spark) to demonstrate a spark example.
 */
public final class JavaSparkSQLExample {

  /**
   * Private constructor.
   */
  private JavaSparkSQLExample() {
  }

  /**
   * Simple person class.
   */
  public static final class Person implements Serializable {
    private String name;
    private int age;

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
     * @return age.
     */
    public int getAge() {
      return age;
    }

    /**
     * Setter.
     * @param age age.
     */
    public void setAge(final int age) {
      this.age = age;
    }
  }

  /**
   * Main function.
   * @param args arguments.
   * @throws AnalysisException Exception.
   */
  public static void main(final String[] args) throws AnalysisException {
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate();

    runBasicDataFrameExample(spark, args[0]);
    runDatasetCreationExample(spark, args[0]);
    runInferSchemaExample(spark, args[1]);
    runProgrammaticSchemaExample(spark, args[1]);

    spark.stop();
  }

  /**
   * Function to run basic data frame example.
   * @param spark spark session.
   * @param peopleJson path to people json file.
   * @throws AnalysisException exception.
   */
  private static void runBasicDataFrameExample(final SparkSession spark, final String peopleJson)
      throws AnalysisException {
    Dataset<Row> df = spark.read().json(peopleJson);

    // Displays the content of the DataFrame to stdout
    df.show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Print the schema in a tree format
    df.printSchema();
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show();
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select(col("name"), col("age").plus(1)).show();
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    // Select people older than 21
    df.filter(col("age").gt(21)).show();
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // Count people by age
    df.groupBy("age").count().show();
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people");

    Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
    sqlDF.show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people");

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    // Global temporary view is cross-session
    try (final org.apache.spark.sql.SparkSession newSession = spark.newSession()) {
      newSession.sql("SELECT * FROM global_temp.people").show();
    }
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
  }

  /**
   * Function to run data creation example.
   * @param spark spark session.
   * @param peopleJson path to people json file.
   */
  private static void runDatasetCreationExample(final SparkSession spark, final String peopleJson) {
    // Create an instance of a Bean class
    Person person = new Person();
    person.setName("Andy");
    person.setAge(32);

    // Encoders are created for Java beans
    Encoder<Person> personEncoder = Encoders.bean(Person.class);
    Dataset<Person> javaBeanDS = spark.createDataset(
        Collections.singletonList(person),
        personEncoder
    );
    javaBeanDS.show();
    // +---+----+
    // |age|name|
    // +---+----+
    // | 32|Andy|
    // +---+----+

    // Encoders for most common types are provided in class Encoders
    Encoder<Integer> integerEncoder = Encoders.INT();
    Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
    Dataset<Integer> transformedDS = primitiveDS.map(
        (MapFunction<Integer, Integer>) value -> value + 1,
        integerEncoder);
    transformedDS.collect(); // Returns [2, 3, 4]

    // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
    String path = peopleJson;
    Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
    peopleDS.show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
  }

  /**
   * Function to run infer schema example.
   * @param spark spark session.
   * @param peopleTxt path to people txt file.
   */
  private static void runInferSchemaExample(final SparkSession spark, final String peopleTxt) {
    // Create an RDD of Person objects from a text file
    JavaRDD<Person> peopleRDD = spark.read()
        .textFile(peopleTxt)
        .javaRDD()
        .map(line -> {
          String[] parts = line.split(",");
          Person person = new Person();
          person.setName(parts[0]);
          person.setAge(Integer.parseInt(parts[1].trim()));
          return person;
        });

    // Apply a schema to an RDD of JavaBeans to get a DataFrame
    Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people");

    // SQL statements can be run by using the sql methods provided by spark
    Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

    // The columns of a row in the result can be accessed by field index
    Encoder<String> stringEncoder = Encoders.STRING();
    Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        stringEncoder);
    teenagerNamesByIndexDF.show();
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
    Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
        stringEncoder);
    teenagerNamesByFieldDF.show();
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
  }

  /**
   * Function to run programmatic schema example.
   * @param spark spark session.
   * @param peopleTxt path to people txt file.
   */
  private static void runProgrammaticSchemaExample(final SparkSession spark, final String peopleTxt) {
    // Create an RDD
    JavaRDD<String> peopleRDD = spark.read()
        .textFile(peopleTxt)
        .toJavaRDD();

    // The schema is encoded in a string
    String schemaString = "name age";

    // Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(" ")) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);

    // Convert records of the RDD (people) to Rows
    JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
      String[] attributes = record.split(",");
      return RowFactory.create(attributes[0], attributes[1].trim());
    });

    // Apply the schema to the RDD
    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

    // Creates a temporary view using the DataFrame
    peopleDataFrame.createOrReplaceTempView("people");

    // SQL can be run over a temporary view created using DataFrames
    Dataset<Row> results = spark.sql("SELECT name FROM people");

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    Dataset<String> namesDS = results.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        Encoders.STRING());
    namesDS.show();
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoRunner;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A simple SQL application.
 * (Copied/Refined from the example code in the Beam repository)
 */
public final class SimpleSumSQL {
  /**
   * Private Constructor.
   */
  private SimpleSumSQL() {
  }

  /**
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String outputFilePath = args[0];

    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoRunner.class);
    options.setJobName("SimpleSumSQL");
    final Pipeline p = Pipeline.create(options);

    // define the input row format
    final Schema schema = Schema.builder()
      .addInt32Field("c1")
      .addStringField("c2")
      .addDoubleField("c3").build();

    // 10 rows with 0 ~ 9.
    final List<Row> rows = IntStream.range(0, 10)
      .mapToObj(i -> Row.withSchema(schema).addValues(i, "row", (double) i).build())
      .collect(Collectors.toList());

    // Create a source PCollection
    final PCollection<Row> inputTable = PBegin.in(p).apply(Create.of(rows).withCoder(schema.getRowCoder()));

    // Run 2 SQL queries
    // ==> Sum of ints larger than 1
    final PCollection<Row> firstQueryResult =
      inputTable.apply(SqlTransform.query("select c1, c2, c3 from PCOLLECTION where c1 > 1"));
    final PCollection<Row> secondQueryResult = PCollectionTuple
      .of(new TupleTag<>("FIRST_QUERY_RESULT"), firstQueryResult)
      .apply(SqlTransform.query("select c2, sum(c3) from FIRST_QUERY_RESULT group by c2"));

    // Write results to a file
    // The result should be 2 + 3 + 4 + ... + 9 = 44
    GenericSourceSink.write(secondQueryResult.apply(MapElements.via(new SimpleFunction<Row, String>() {
      @Override
      public String apply(final Row input) {
        final String c2 = input.getString(0);
        final Double c3 = input.getDouble(1);
        return c2 + " is " + c3;
      }
    })), outputFilePath);

    p.run();
  }
}

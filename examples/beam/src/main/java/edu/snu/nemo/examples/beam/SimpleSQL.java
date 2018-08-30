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
package edu.snu.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import javax.annotation.Nullable;

/**
 * A simple SQL application.
 */
public final class SimpleSQL {
  /**
   * Private Constructor.
   */
  private SimpleSQL() {
  }

  /**
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    final Pipeline p = Pipeline.create(options);

    // define the input row format
    final Schema type = Schema.builder()
      .addInt32Field("c1")
      .addStringField("c2")
      .addDoubleField("c3")
      .build();

    final Row row1 = Row.withSchema(type).addValues(1, "row", 1.0).build();
    final Row row2 = Row.withSchema(type).addValues(2, "row", 2.0).build();
    final Row row3 = Row.withSchema(type).addValues(3, "row", 3.0).build();

    // Create a source PCollection
    final PCollection<Row> inputTable =
      PBegin.in(p)
        .apply(
          Create.of(row1, row2, row3)
            .withSchema(
              type, SerializableFunctions.identity(), SerializableFunctions.identity()));

    // Case 1. run a simple SQL query, and print
    final PCollection<Row> caseOneResult =
      inputTable.apply(SqlTransform.query("select c1, c2, c3 from PCOLLECTION where c1 > 1"));
    caseOneResult.apply(MapElements.via(new SimpleFunction<Row, Void>() {
      @Override
      public @Nullable Void apply(Row input) {
        // expect output:
        //  PCOLLECTION: [3, row, 3.0]
        //  PCOLLECTION: [2, row, 2.0]
        System.out.println("CASE 1: " + input.getValues());
        return null;
      }
    }));

    // Case 2. run another query over CASE1_RESULT, and print
    final PCollection<Row> outputStream2 = PCollectionTuple.of(new TupleTag<>("CASE1_RESULT"), caseOneResult)
      .apply(SqlTransform.query("select c2, sum(c3) from CASE1_RESULT group by c2"));
    outputStream2.apply(MapElements.via(new SimpleFunction<Row, Void>() {
      @Override
      public @Nullable Void apply(Row input) {
        // expect output:
        //  CASE1_RESULT: [row, 5.0]
        System.out.println("CASE 2: " + input.getValues());
        return null;
      }
    }));

    p.run().waitUntilFinish();
  }
}

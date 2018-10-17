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
package org.apache.nemo.examples.beam.tpch;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.nemo.examples.beam.GenericSourceSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * TPC-H query runner.
 * (Copied and adapted from https://github.com/apache/beam/pull/6240)
 */
public final class Tpch {
  private static final Logger LOG = LoggerFactory.getLogger(Tpch.class.getName());

  /**
   * Private Constructor.
   */
  private Tpch() {
  }

  private static PCollectionTuple getHTables(final Pipeline pipeline,
                                             final CSVFormat csvFormat,
                                             final String inputDirectory,
                                             final String query) {
    final ImmutableMap<String, Schema> hSchemas = ImmutableMap.<String, Schema>builder()
      .put("lineitem", Schemas.LINEITEM_SCHEMA)
      .put("customer", Schemas.CUSTOMER_SCHEMA)
      .put("orders", Schemas.ORDER_SCHEMA)
      .put("supplier", Schemas.SUPPLIER_SCHEMA)
      .put("nation", Schemas.NATION_SCHEMA)
      .put("region", Schemas.REGION_SCHEMA)
      .put("part", Schemas.PART_SCHEMA)
      .put("partsupp", Schemas.PARTSUPP_SCHEMA)
      .build();

    PCollectionTuple tables = PCollectionTuple.empty(pipeline);
    for (final Map.Entry<String, Schema> tableSchema : hSchemas.entrySet()) {
      final String tableName = tableSchema.getKey();
      if (query.contains(tableName)) {
        final String filePattern = inputDirectory + tableSchema.getKey() + ".tbl(|*)";
        final PCollection<Row> table = GenericSourceSink.read(pipeline, filePattern)
          .apply("StringToRow", new TextTableProvider.CsvToRow(tableSchema.getValue(), csvFormat))
          .setCoder(tableSchema.getValue().getRowCoder())
          .setName(tableSchema.getKey());
        tables = tables.and(new TupleTag<>(tableSchema.getKey()), table);
        LOG.info("Will load table {} from {}", tableName, filePattern);
      }
    }
    return tables;
  }


  /**
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String queryFilePath = args[0];
    final String inputDirectory = args[1];
    final String outputFilePath = args[2];

    LOG.info("{} / {} / {}", queryFilePath, inputDirectory, outputFilePath);

    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("TPC-H");
    final Pipeline p = Pipeline.create(options);

    final String queryString = getQueryString(queryFilePath);

    // Create tables.
    final CSVFormat csvFormat = CSVFormat.MYSQL
      .withDelimiter('|')
      .withNullString("")
      .withTrailingDelimiter();
    final PCollectionTuple tables = getHTables(p, csvFormat, inputDirectory, queryString);

    // Run the TPC-H query.
    final PCollection<Row> result = tables.apply(SqlTransform.query(queryString));

    // Write the results.
    final PCollection<String> resultToWrite = result
      .apply(MapElements.into(TypeDescriptors.strings()).via(input -> input.getValues().toString()));
    GenericSourceSink.write(resultToWrite, outputFilePath);

    // Run the pipeline.
    p.run();
  }

  private static String getQueryString(final String queryFilePath) {
    final List<String> lines = new ArrayList<>();
    try (final Stream<String> stream  = Files.lines(Paths.get(queryFilePath))) {
      stream.forEach(lines::add);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final StringBuilder sb = new StringBuilder();
    lines.forEach(line -> {
      sb.append(" ");
      sb.append(line);
    });
    final String concate = sb.toString();

    final String cleanOne = concate.replaceAll("\n", " ");
    final String cleanTwo = cleanOne.replaceAll("\t", " ");

    LOG.info("Will execute SQL file {} with query: {}", queryFilePath, cleanTwo);

    return cleanTwo;
  }
}

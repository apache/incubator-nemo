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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.client.beam.NemoRunner;
import org.apache.nemo.examples.beam.GenericSourceSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRow2CsvLine;

/**
 * A simple SQL application.
 * (Copied and adapted from https://github.com/apache/beam/pull/6240)
 */
public final class Tpch {
  private static final Logger LOG = LoggerFactory.getLogger(Tpch.class.getName());

  /**
   * Private Constructor.
   */
  private Tpch() {
  }

  /**
   * Row csv formats.
   */
  static class RowToCsv extends PTransform<PCollection<Row>, PCollection<String>> implements Serializable {

    private final CSVFormat csvFormat;

    RowToCsv(final CSVFormat csvFormat) {
      this.csvFormat = csvFormat;
    }

    public CSVFormat getCsvFormat() {
      return csvFormat;
    }

    @Override
    public PCollection<String> expand(final PCollection<Row> input) {
      return input.apply(
        "rowToCsv",
        MapElements.into(TypeDescriptors.strings()).via(row -> beamRow2CsvLine(row, csvFormat)));
    }
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
      /*
      .put("store_sales", Schemas.STORE_SALES_SCHEMA)
      .put("catalog_sales", Schemas.CATALOG_SALES_SCHEMA)
      .put("item", Schemas.ITEM_SCHEMA)
      .put("date_dim", Schemas.DATE_DIM_SCHEMA)
      .put("promotion", Schemas.PROMOTION_SCHEMA)
      .put("customer_demographics", Schemas.CUSTOMER_DEMOGRAPHIC_SCHEMA)
      .put("web_sales", Schemas.WEB_SALES_SCHEMA)
      .put("inventory", Schemas.INVENTORY_SCHEMA)
      */
      .build();

    PCollectionTuple tables = PCollectionTuple.empty(pipeline);
    for (final Map.Entry<String, Schema> tableSchema : hSchemas.entrySet()) {
      final String tableName = tableSchema.getKey();

      if (query.contains(tableName)) {
        LOG.info("HIT: tablename {}", tableName);

        final String filePattern = inputDirectory + tableSchema.getKey() + ".tbl*";
        final PCollection<Row> table = GenericSourceSink.read(pipeline, filePattern)
          .apply("StringToRow", new TextTableProvider.CsvToRow(tableSchema.getValue(), csvFormat))
          .setCoder(tableSchema.getValue().getRowCoder())
          .setName(tableSchema.getKey());
        tables = tables.and(new TupleTag<>(tableSchema.getKey()), table);

        LOG.info("FilePattern {} / Tables {}", filePattern, tables);
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
    options.setRunner(NemoRunner.class);
    options.setJobName("TPC-H");
    final Pipeline p = Pipeline.create(options);

    final String queryString = getQueryString(queryFilePath);
    // Create tables
    final CSVFormat csvFormat = CSVFormat.MYSQL
      .withDelimiter('|')
      .withNullString("")
      .withTrailingDelimiter();
    final PCollectionTuple tables = getHTables(p, csvFormat, inputDirectory, queryString);

    // Run the TPC-H query
    final PCollection<Row> result = tables.apply(SqlTransform.query(queryString));

    final PCollection<String> resultToWrite = result.apply(MapElements.into(TypeDescriptors.strings()).via(
      new SerializableFunction<Row, String>() {
        @Override
        public String apply(final Row input) {
          System.out.println(input.getValues().toString());
          return input.getValues().toString();
        }
      }));

    GenericSourceSink.write(resultToWrite, outputFilePath);

    // Then run
    p.run();
  }

  private static String getQueryString(final String queryFilePath) {
    final List<String> lines = new ArrayList<>();
    try (final Stream<String> stream  = Files.lines(Paths.get(queryFilePath))) {
      stream.forEach(lines::add);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    System.out.println(lines);

    final StringBuilder sb = new StringBuilder();
    lines.forEach(line -> {
      sb.append(" ");
      sb.append(line);
    });

    final String concate = sb.toString();
    System.out.println(concate);
    final String cleanOne = concate.replaceAll("\n", " ");
    System.out.println(cleanOne);
    final String cleanTwo = cleanOne.replaceAll("\t", " ");
    System.out.println(cleanTwo);

    return cleanTwo;
  }
}

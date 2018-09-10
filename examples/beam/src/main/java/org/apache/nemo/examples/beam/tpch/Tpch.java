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
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTable;
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
import org.apache.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.nemo.examples.beam.GenericSourceSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRow2CsvLine;

/**
 * A simple SQL application.
 * (Copied and adapted from https://github.com/apache/beam/pull/6240)
 */
public final class Tpch {
  private static final Logger LOG = LoggerFactory.getLogger(Tpch.class.getName());

  public static final String QUERY1 =
    "select\n"
      + "\tl_returnflag,\n"
      + "\tl_linestatus,\n"
      + "\tsum(l_quantity) as sum_qty,\n"
      + "\tsum(l_extendedprice) as sum_base_price,\n"
      + "\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n"
      + "\tsum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n"
      + "\tavg(l_quantity) as avg_qty,\n"
      + "\tavg(l_extendedprice) as avg_price,\n"
      + "\tavg(l_discount) as avg_disc,\n"
      + "\tcount(*) as count_order\n"
      + "from\n"
      + "\tlineitem\n"
      + "where\n"
      + "\tl_shipdate <= date '1998-12-01' - interval '90' day (3)\n"
      + "group by\n"
      + "\tl_returnflag,\n"
      + "\tl_linestatus\n"
      + "order by\n"
      + "\tl_returnflag,\n"
      + "\tl_linestatus limit 10";

  public static final String QUERY3 =
    "select\n"
      + "\tl_orderkey,\n"
      + "\tsum(l_extendedprice * (1 - l_discount)) as revenue,\n"
      + "\to_orderdate,\n"
      + "\to_shippriority\n"
      + "from\n"
      + "\tcustomer,\n"
      + "\torders,\n"
      + "\tlineitem\n"
      + "where\n"
      + "\tc_mktsegment = 'BUILDING'\n"
      + "\tand c_custkey = o_custkey\n"
      + "\tand l_orderkey = o_orderkey\n"
      + "\tand o_orderdate < date '1995-03-15'\n"
      + "\tand l_shipdate > date '1995-03-15'\n"
      + "group by\n"
      + "\tl_orderkey,\n"
      + "\to_orderdate,\n"
      + "\to_shippriority\n"
      + "order by\n"
      + "\trevenue desc,\n"
      + "\to_orderdate\n"
      + "limit 10";

  public static final String QUERY4 =
    "select\n"
      + "\to_orderpriority,\n"
      + "\tcount(*) as order_count\n"
      + "from\n"
      + "\torders\n"
      + "where\n"
      + "\to_orderdate >= date '1993-07-01'\n"
      + "\tand o_orderdate < date '1993-07-01' + interval '3' month\n"
      + "\tand exists (\n"
      + "\t\tselect\n"
      + "\t\t\t*\n"
      + "\t\tfrom\n"
      + "\t\t\tlineitem\n"
      + "\t\twhere\n"
      + "\t\t\tl_orderkey = o_orderkey\n"
      + "\t\t\tand l_commitdate < l_receiptdate\n"
      + "\t)\n"
      + "group by\n"
      + "\to_orderpriority\n"
      + "order by\n"
      + "\to_orderpriority limit 10";

  public static final String QUERY5 =
    "select\n"
      + "\tn_name,\n"
      + "\tsum(l_extendedprice * (1 - l_discount)) as revenue\n"
      + "from\n"
      + "\tcustomer,\n"
      + "\torders,\n"
      + "\tlineitem,\n"
      + "\tsupplier,\n"
      + "\tnation,\n"
      + "\tregion\n"
      + "where\n"
      + "\tc_custkey = o_custkey\n"
      + "\tand l_orderkey = o_orderkey\n"
      + "\tand l_suppkey = s_suppkey\n"
      + "\tand c_nationkey = s_nationkey\n"
      + "\tand s_nationkey = n_nationkey\n"
      + "\tand n_regionkey = r_regionkey\n"
      + "\tand r_name = 'ASIA'\n"
      + "\tand o_orderdate >= date '1994-01-01'\n"
      + "\tand o_orderdate < date '1994-01-01' + interval '1' year\n"
      + "group by\n"
      + "\tn_name\n"
      + "order by\n"
      + "\trevenue desc limit 10";

  public static final String QUERY6 =
    "select\n"
      + "\tsum(l_extendedprice * l_discount) as revenue\n"
      + "from\n"
      + "\tlineitem\n"
      + "where\n"
      + "\tl_shipdate >= date '1994-01-01'\n"
      + "\tand l_shipdate < date '1994-01-01' + interval '1' year\n"
      + "\tand l_discount between .06 - 0.01 and .06 + 0.01\n"
      + "\tand l_quantity < 24 limit 10";


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
                                             final String inputDirectory) {
    final ImmutableMap<String, Schema> hSchemas = ImmutableMap.<String, Schema>builder()
      .put("lineitem", Schemas.LINEITEM_SCHEMA)
      .put("customer", Schemas.CUSTOMER_SCHEMA)
      .put("orders", Schemas.ORDER_SCHEMA)

      .put("supplier", Schemas.SUPPLIER_SCHEMA)
      .put("nation", Schemas.NATION_SCHEMA)
      .put("region", Schemas.REGION_SCHEMA)

      /*
      .put("part", Schemas.PART_SCHEMA)
      .put("partsupp", Schemas.PARTSUPP_SCHEMA)
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
      final String filePattern = inputDirectory + tableSchema.getKey() + ".tbl";
      final PCollection<Row> table = new TextTable(
        tableSchema.getValue(),
        filePattern,
        new TextTableProvider.CsvToRow(tableSchema.getValue(), csvFormat),
        new RowToCsv(csvFormat))
        .buildIOReader(pipeline.begin())
        .setCoder(tableSchema.getValue().getRowCoder())
        .setName(tableSchema.getKey());
      tables = tables.and(new TupleTag<>(tableSchema.getKey()), table);

      LOG.info("FilePattern {} / Tables {}", filePattern, tables);
    }
    return tables;
  }


  /**
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final int queryId = Integer.valueOf(args[0]);
    final String inputDirectory = args[1];
    final String outputFilePath = args[2];

    final Map<Integer, String> idToQuery = new HashMap<>();
    idToQuery.put(1, QUERY1);
    idToQuery.put(3, QUERY3);
    idToQuery.put(4, QUERY4);
    idToQuery.put(5, QUERY5);
    idToQuery.put(6, QUERY6);

    LOG.info("{} / {}", queryId, inputDirectory, outputFilePath);

    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("TPC-H");
    final Pipeline p = Pipeline.create(options);

    // Create tables
    final CSVFormat csvFormat = CSVFormat.MYSQL
      .withDelimiter('|')
      .withNullString("")
      .withTrailingDelimiter();
    final PCollectionTuple tables = getHTables(p, csvFormat, inputDirectory);

    // Run the TPC-H query
    final PCollection<Row> result = tables.apply(SqlTransform.query(idToQuery.get(queryId)));

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
}

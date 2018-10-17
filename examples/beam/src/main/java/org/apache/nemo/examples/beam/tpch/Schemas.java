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

import org.apache.beam.sdk.schemas.Schema;

/**
 * TPC-H Schemas.
 * (Copied and adapted from https://github.com/apache/beam/pull/6240)
 */
final class Schemas {
  /**
   * Private.
   */
  private Schemas() {
  }

  static final Schema ORDER_SCHEMA =
    Schema.builder()
      .addInt32Field("o_orderkey")
      .addInt32Field("o_custkey")
      .addStringField("o_orderstatus")
      .addFloatField("o_totalprice")
      .addStringField("o_orderdate")
      .addStringField("o_orderpriority")
      .addStringField("o_clerk")
      .addInt32Field("o_shippriority")
      .addStringField("o_comment")
      .build();

  static final Schema CUSTOMER_SCHEMA =
    Schema.builder()
      .addInt32Field("c_custkey")
      .addStringField("c_name")
      .addStringField("c_address")
      .addInt32Field("c_nationkey")
      .addStringField("c_phone")
      .addFloatField("c_acctbal")
      .addStringField("c_mktsegment")
      .addStringField("c_comment")
      .build();

  static final Schema LINEITEM_SCHEMA =
    Schema.builder()
      .addInt32Field("l_orderkey")
      .addInt32Field("l_partkey")
      .addInt32Field("l_suppkey")
      .addInt32Field("l_linenumber")
      .addFloatField("l_quantity")
      .addFloatField("l_extendedprice")
      .addFloatField("l_discount")
      .addFloatField("l_tax")
      .addStringField("l_returnflag")
      .addStringField("l_linestatus")
      .addStringField("l_shipdate")
      .addStringField("l_commitdate")
      .addStringField("l_receiptdate")
      .addStringField("l_shipinstruct")
      .addStringField("l_shipmode")
      .addStringField("l_comment")
      .build();

  static final Schema PARTSUPP_SCHEMA =
    Schema.builder()
      .addInt32Field("ps_partkey")
      .addInt32Field("ps_suppkey")
      .addInt32Field("ps_availqty")
      .addFloatField("ps_supplycost")
      .addStringField("ps_comment")
      .build();

  static final Schema REGION_SCHEMA =
    Schema.builder()
      .addInt32Field("r_regionkey")
      .addStringField("r_name")
      .addStringField("r_comment")
      .build();

  static final Schema SUPPLIER_SCHEMA =
    Schema.builder()
      .addInt32Field("s_suppkey")
      .addStringField("s_name")
      .addStringField("s_address")
      .addInt32Field("s_nationkey")
      .addStringField("s_phone")
      .addFloatField("s_acctbal")
      .addStringField("s_comment")
      .build();

  static final Schema PART_SCHEMA =
    Schema.builder()
      .addInt32Field("p_partkey")
      .addStringField("p_name")
      .addStringField("p_mfgr")
      .addStringField("p_brand")
      .addStringField("p_type")
      .addInt32Field("p_size")
      .addStringField("p_container")
      .addFloatField("p_retailprice")
      .addStringField("p_comment")
      .build();

  static final Schema NATION_SCHEMA =
    Schema.builder()
      .addInt32Field("n_nationkey")
      .addStringField("n_name")
      .addInt32Field("n_regionkey")
      .addStringField("n_comment")
      .build();
}

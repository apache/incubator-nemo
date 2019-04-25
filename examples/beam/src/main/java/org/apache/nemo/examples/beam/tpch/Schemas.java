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
import org.apache.beam.sdk.schemas.Schema;

/**
 * A simple SQL application.
 * (Copied and adapted from https://github.com/apache/beam/pull/6240)
 */
public final class Schemas {
  /**
   * Private.
   */
  private Schemas() {
  }

  public static final ImmutableMap<String, String> COLUMN_PREFIX = ImmutableMap.<String, String>builder()
    .put("lineitem", "l_")
    .put("customer", "c_")
    .put("supplier", "s_")
    .put("partsupp", "ps_")
    .put("part", "p_")
    .put("orders", "o_")
    .put("nation", "n_")
    .put("region", "r_")
    .build();

  public static final Schema STORE_SALES_SCHEMA =
    Schema.builder()
      .addNullableField("ss_sold_date_sk", Schema.FieldType.INT64)
      .addNullableField("ss_sold_time_sk", Schema.FieldType.INT64)
      .addNullableField("ss_item_sk", Schema.FieldType.INT64)
      .addNullableField("ss_customer_sk", Schema.FieldType.STRING)
      .addNullableField("ss_cdemo_sk", Schema.FieldType.INT64)
      .addNullableField("ss_hdemo_sk", Schema.FieldType.INT64)
      .addNullableField("ss_addr_sk", Schema.FieldType.INT64)
      .addNullableField("ss_store_sk", Schema.FieldType.INT64)
      .addNullableField("ss_promo_sk", Schema.FieldType.INT64)
      .addNullableField("ss_ticket_number", Schema.FieldType.INT64)
      .addNullableField("ss_quantity", Schema.FieldType.INT64)
      .addNullableField("ss_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("ss_list_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_discount_amt", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_list_price", Schema.FieldType.FLOAT)
      .addNullableField("ss_ext_tax", Schema.FieldType.FLOAT)
      .addNullableField("ss_coupon_amt", Schema.FieldType.FLOAT)
      .addNullableField("ss_net_paid", Schema.FieldType.FLOAT)
      .addNullableField("ss_net_paid_inc_tax", Schema.FieldType.FLOAT)
      .addNullableField("ss_net_profit", Schema.FieldType.FLOAT)
      .build();

  public static final Schema DATE_DIM_SCHEMA =
    Schema.builder()
      .addNullableField("d_date_sk", Schema.FieldType.INT64)
      .addNullableField("d_date_id", Schema.FieldType.STRING)
      .addNullableField("d_date", Schema.FieldType.STRING)
      .addNullableField("d_month_seq", Schema.FieldType.INT64)
      .addNullableField("d_week_seq", Schema.FieldType.INT64)
      .addNullableField("d_quarter_seq", Schema.FieldType.INT64)
      .addNullableField("d_year", Schema.FieldType.INT64)
      .addNullableField("d_dow", Schema.FieldType.INT64)
      .addNullableField("d_moy", Schema.FieldType.INT64)
      .addNullableField("d_dom", Schema.FieldType.INT64)
      .addNullableField("d_qoy", Schema.FieldType.INT64)
      .addNullableField("d_fy_year", Schema.FieldType.INT64)
      .addNullableField("d_fy_quarter_seq", Schema.FieldType.INT64)
      .addNullableField("d_fy_week_seq", Schema.FieldType.INT64)
      .addNullableField("d_day_name", Schema.FieldType.STRING)
      .addNullableField("d_quarter_name", Schema.FieldType.STRING)
      .addNullableField("d_holiday", Schema.FieldType.STRING)
      .addNullableField("d_weekend", Schema.FieldType.STRING)
      .addNullableField("d_following_holiday", Schema.FieldType.STRING)
      .addNullableField("d_first_dom", Schema.FieldType.INT64)
      .addNullableField("d_last_dom", Schema.FieldType.INT64)
      .addNullableField("d_same_day_ly", Schema.FieldType.INT64)
      .addNullableField("d_same_day_lq", Schema.FieldType.INT64)
      .addNullableField("d_current_day", Schema.FieldType.STRING)
      .addNullableField("d_current_week", Schema.FieldType.STRING)
      .addNullableField("d_current_month", Schema.FieldType.STRING)
      .addNullableField("d_current_quarter", Schema.FieldType.STRING)
      .addNullableField("d_current_year", Schema.FieldType.STRING)
      .build();

  public static final Schema ITEM_SCHEMA =
    Schema.builder()
      .addNullableField("i_item_sk", Schema.FieldType.INT64)
      .addNullableField("i_item_id", Schema.FieldType.STRING)
      .addNullableField("i_rec_start_date", Schema.FieldType.DATETIME)
      .addNullableField("i_rec_end_date", Schema.FieldType.DATETIME)
      .addNullableField("i_item_desc", Schema.FieldType.STRING)
      .addNullableField("i_current_price", Schema.FieldType.FLOAT)
      .addNullableField("i_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("i_brand_id", Schema.FieldType.INT64)
      .addNullableField("i_brand", Schema.FieldType.STRING)
      .addNullableField("i_class_id", Schema.FieldType.INT64)
      .addNullableField("i_class", Schema.FieldType.STRING)
      .addNullableField("i_category_id", Schema.FieldType.INT64)
      .addNullableField("i_category", Schema.FieldType.STRING)
      .addNullableField("i_manufact_id", Schema.FieldType.INT64)
      .addNullableField("i_manufact", Schema.FieldType.STRING)
      .addNullableField("i_size", Schema.FieldType.STRING)
      .addNullableField("i_formulation", Schema.FieldType.STRING)
      .addNullableField("i_color", Schema.FieldType.STRING)
      .addNullableField("i_units", Schema.FieldType.STRING)
      .addNullableField("i_container", Schema.FieldType.STRING)
      .addNullableField("i_manager_id", Schema.FieldType.INT64)
      .addNullableField("i_product_name", Schema.FieldType.STRING)
      .build();

  public static final Schema INVENTORY_SCHEMA =
    Schema.builder()
      .addNullableField("inv_date_sk", Schema.FieldType.INT64)
      .addNullableField("inv_item_sk", Schema.FieldType.INT64)
      .addNullableField("inv_warehouse_sk", Schema.FieldType.INT64)
      .addNullableField("inv_quantity_on_hand", Schema.FieldType.INT64)
      .build();

  public static final Schema CATALOG_SALES_SCHEMA =
    Schema.builder()
      .addNullableField("cs_sold_date_sk", Schema.FieldType.INT64)
      .addNullableField("cs_sold_time_sk", Schema.FieldType.INT64)
      .addNullableField("cs_ship_date_sk", Schema.FieldType.INT64)
      .addNullableField("cs_bill_customer_sk", Schema.FieldType.INT64)
      .addNullableField("cs_bill_cdemo_sk", Schema.FieldType.INT64)
      .addNullableField("cs_bill_hdemo_sk", Schema.FieldType.INT64)
      .addNullableField("cs_bill_addr_sk", Schema.FieldType.INT64)
      .addNullableField("cs_ship_customer_sk", Schema.FieldType.INT64)
      .addNullableField("cs_ship_cdemo_sk", Schema.FieldType.INT64)
      .addNullableField("cs_ship_hdemo_sk", Schema.FieldType.INT64)
      .addNullableField("cs_ship_addr_sk", Schema.FieldType.INT64)
      .addNullableField("cs_call_center_sk", Schema.FieldType.INT64)
      .addNullableField("cs_catalog_page_sk", Schema.FieldType.INT64)
      .addNullableField("cs_ship_mode_sk", Schema.FieldType.INT64)
      .addNullableField("cs_warehouse_sk", Schema.FieldType.INT64)
      .addNullableField("cs_item_sk", Schema.FieldType.INT64)
      .addNullableField("cs_promo_sk", Schema.FieldType.INT64)
      .addNullableField("cs_order_number", Schema.FieldType.INT64)
      .addNullableField("cs_quantity", Schema.FieldType.INT64)
      .addNullableField("cs_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("cs_list_price", Schema.FieldType.FLOAT)
      .addNullableField("cs_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("cs_ext_discount_amt", Schema.FieldType.FLOAT)
      .addNullableField("cs_ext_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("cs_ext_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("cs_ext_list_price", Schema.FieldType.FLOAT)
      .addNullableField("cs_ext_tax", Schema.FieldType.FLOAT)
      .addNullableField("cs_coupon_amt", Schema.FieldType.FLOAT)
      .addNullableField("cs_ext_ship_cost", Schema.FieldType.FLOAT)
      .addNullableField("cs_net_paid", Schema.FieldType.FLOAT)
      .addNullableField("cs_net_paid_inc_tax", Schema.FieldType.FLOAT)
      .addNullableField("cs_net_paid_inc_ship", Schema.FieldType.FLOAT)
      .addNullableField("cs_net_paid_inc_ship_tax", Schema.FieldType.FLOAT)
      .addNullableField("cs_net_profit", Schema.FieldType.FLOAT)
      .build();

  public static final Schema ORDER_SCHEMA =
    Schema.builder()
      .addInt64Field("o_orderkey")
      .addInt64Field("o_custkey")
      .addStringField("o_orderstatus")
      .addFloatField("o_totalprice")
      .addStringField("o_orderdate")
      .addStringField("o_orderpriority")
      .addStringField("o_clerk")
      .addInt64Field("o_shippriority")
      .addStringField("o_comment")
      .build();

  public static final Schema CUSTOMER_SCHEMA =
    Schema.builder()
      .addInt64Field("c_custkey")
      .addStringField("c_name")
      .addStringField("c_address")
      .addInt64Field("c_nationkey")
      .addStringField("c_phone")
      .addFloatField("c_acctbal")
      .addStringField("c_mktsegment")
      .addStringField("c_comment")
      .build();

  public static final Schema CUSTOMER_DS_SCHEMA =
    Schema.builder()
      .addNullableField("c_customer_sk", Schema.FieldType.INT64)
      .addNullableField("c_customer_id", Schema.FieldType.STRING)
      .addNullableField("c_current_cdemo_sk", Schema.FieldType.INT64)
      .addNullableField("c_current_hdemo_sk", Schema.FieldType.INT64)
      .addNullableField("c_current_addr_sk", Schema.FieldType.INT64)
      .addNullableField("c_first_shipto_date_sk", Schema.FieldType.INT64)
      .addNullableField("c_first_sales_date_sk", Schema.FieldType.INT64)
      .addNullableField("c_salutation", Schema.FieldType.STRING)
      .addNullableField("c_first_name", Schema.FieldType.STRING)
      .addNullableField("c_last_name", Schema.FieldType.STRING)
      .addNullableField("c_preferred_cust_flag", Schema.FieldType.STRING)
      .addNullableField("c_birth_day", Schema.FieldType.INT64)
      .addNullableField("c_birth_month", Schema.FieldType.INT64)
      .addNullableField("c_birth_year", Schema.FieldType.INT64)
      .addNullableField("c_birth_country", Schema.FieldType.STRING)
      .addNullableField("c_login", Schema.FieldType.STRING)
      .addNullableField("c_email_address", Schema.FieldType.STRING)
      .addNullableField("c_last_review_date", Schema.FieldType.STRING)
      .build();

  public static final Schema LINEITEM_SCHEMA =
    Schema.builder()
      .addInt64Field("l_orderkey")
      .addInt64Field("l_partkey")
      .addInt64Field("l_suppkey")
      .addInt64Field("l_linenumber")
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

  public static final Schema PARTSUPP_SCHEMA =
    Schema.builder()
      .addInt64Field("ps_partkey")
      .addInt64Field("ps_suppkey")
      .addInt64Field("ps_availqty")
      .addFloatField("ps_supplycost")
      .addStringField("ps_comment")
      .build();

  public static final Schema REGION_SCHEMA =
    Schema.builder()
      .addInt64Field("r_regionkey")
      .addStringField("r_name")
      .addStringField("r_comment")
      .build();

  public static final Schema SUPPLIER_SCHEMA =
    Schema.builder()
      .addInt64Field("s_suppkey")
      .addStringField("s_name")
      .addStringField("s_address")
      .addInt64Field("s_nationkey")
      .addStringField("s_phone")
      .addFloatField("s_acctbal")
      .addStringField("s_comment")
      .build();

  public static final Schema PART_SCHEMA =
    Schema.builder()
      .addInt64Field("p_partkey")
      .addStringField("p_name")
      .addStringField("p_mfgr")
      .addStringField("p_brand")
      .addStringField("p_type")
      .addInt64Field("p_size")
      .addStringField("p_container")
      .addFloatField("p_retailprice")
      .addStringField("p_comment")
      .build();

  public static final Schema NATION_SCHEMA =
    Schema.builder()
      .addInt64Field("n_nationkey")
      .addStringField("n_name")
      .addInt64Field("n_regionkey")
      .addStringField("n_comment")
      .build();

  public static final Schema PROMOTION_SCHEMA =
    Schema.builder()
      .addNullableField("p_promo_sk", Schema.FieldType.INT64)
      .addNullableField("p_promo_id", Schema.FieldType.STRING)
      .addNullableField("p_start_date_sk", Schema.FieldType.INT64)
      .addNullableField("p_end_date_sk", Schema.FieldType.INT64)
      .addNullableField("p_item_sk", Schema.FieldType.INT64)
      .addNullableField("p_cost", Schema.FieldType.FLOAT)
      .addNullableField("p_response_target", Schema.FieldType.INT64)
      .addNullableField("p_promo_name", Schema.FieldType.STRING)
      .addNullableField("p_channel_dmail", Schema.FieldType.STRING)
      .addNullableField("p_channel_email", Schema.FieldType.STRING)
      .addNullableField("p_channel_catalog", Schema.FieldType.STRING)
      .addNullableField("p_channel_tv", Schema.FieldType.STRING)
      .addNullableField("p_channel_radio", Schema.FieldType.STRING)
      .addNullableField("p_channel_press", Schema.FieldType.STRING)
      .addNullableField("p_channel_event", Schema.FieldType.STRING)
      .addNullableField("p_channel_demo", Schema.FieldType.STRING)
      .addNullableField("p_channel_details", Schema.FieldType.STRING)
      .addNullableField("p_purpose", Schema.FieldType.STRING)
      .addNullableField("p_discount_active", Schema.FieldType.STRING)
      .build();

  public static final Schema CUSTOMER_DEMOGRAPHIC_SCHEMA =
    Schema.builder()
      .addNullableField("cd_demo_sk", Schema.FieldType.INT64)
      .addNullableField("cd_gender", Schema.FieldType.STRING)
      .addNullableField("cd_marital_status", Schema.FieldType.STRING)
      .addNullableField("cd_education_status", Schema.FieldType.STRING)
      .addNullableField("cd_purchase_estimate", Schema.FieldType.INT64)
      .addNullableField("cd_credit_rating", Schema.FieldType.STRING)
      .addNullableField("cd_dep_count", Schema.FieldType.INT64)
      .addNullableField("cd_dep_employed_count", Schema.FieldType.INT64)
      .addNullableField("cd_dep_college_count", Schema.FieldType.INT64)
      .build();

  public static final Schema WEB_SALES_SCHEMA =
    Schema.builder()
      .addNullableField("ws_sold_date_sk", Schema.FieldType.INT64)
      .addNullableField("ws_sold_time_sk", Schema.FieldType.INT64)
      .addNullableField("ws_ship_date_sk", Schema.FieldType.INT64)
      .addNullableField("ws_item_sk", Schema.FieldType.INT64)
      .addNullableField("ws_bill_customer_sk", Schema.FieldType.INT64)
      .addNullableField("ws_bill_cdemo_sk", Schema.FieldType.INT64)
      .addNullableField("ws_bill_hdemo_sk", Schema.FieldType.INT64)
      .addNullableField("ws_bill_addr_sk", Schema.FieldType.INT64)
      .addNullableField("ws_ship_customer_sk", Schema.FieldType.INT64)
      .addNullableField("ws_ship_cdemo_sk", Schema.FieldType.INT64)
      .addNullableField("ws_ship_hdemo_sk", Schema.FieldType.INT64)
      .addNullableField("ws_ship_addr_sk", Schema.FieldType.INT64)
      .addNullableField("ws_web_page_sk", Schema.FieldType.INT64)
      .addNullableField("ws_web_site_sk", Schema.FieldType.INT64)
      .addNullableField("ws_ship_mode_sk", Schema.FieldType.INT64)
      .addNullableField("ws_warehouse_sk", Schema.FieldType.INT64)
      .addNullableField("ws_promo_sk", Schema.FieldType.INT64)
      .addNullableField("ws_order_number", Schema.FieldType.INT64)
      .addNullableField("ws_quantity", Schema.FieldType.INT64)
      .addNullableField("ws_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("ws_list_price", Schema.FieldType.FLOAT)
      .addNullableField("ws_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("ws_ext_discount_amt", Schema.FieldType.FLOAT)
      .addNullableField("ws_ext_sales_price", Schema.FieldType.FLOAT)
      .addNullableField("ws_ext_wholesale_cost", Schema.FieldType.FLOAT)
      .addNullableField("ws_ext_list_price", Schema.FieldType.FLOAT)
      .addNullableField("ws_ext_tax", Schema.FieldType.FLOAT)
      .addNullableField("ws_coupon_amt", Schema.FieldType.FLOAT)
      .addNullableField("ws_ext_ship_cost", Schema.FieldType.FLOAT)
      .addNullableField("ws_net_paid", Schema.FieldType.FLOAT)
      .addNullableField("ws_net_paid_inc_tax", Schema.FieldType.FLOAT)
      .addNullableField("ws_net_paid_inc_ship", Schema.FieldType.FLOAT)
      .addNullableField("ws_net_paid_inc_ship_tax", Schema.FieldType.FLOAT)
      .addNullableField("ws_net_profit", Schema.FieldType.FLOAT)
      .build();
}

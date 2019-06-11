#!/usr/bin/env bash

EMR_HOME="user.cootek/imecore"
EMR_OUT="$EMR_HOME"/out
JAR_PATH="$EMR_HOME"/jar
IP2LOCATION_DATA_PATH="$EMR_HOME"/data/IP-COUNTRY-REGION-CITY.BIN

LOCAL_TMP_DIR="/tmp"


DW_ROOT="user.cootek/data_warehouse"
DW_ACTIVATE_JSON="$DW_ROOT"/activate/daily/smartinput/json
DW_ACTIVATE_PARQUET="$DW_ROOT"/activate/daily/smartinput/parquet

DW_ACTIVATE_SUMMARY_JSON="$DW_ROOT"/activate/summary/rowkey_based/json/\{eu,ap,us\}
DW_ACTIVATE_SUMMARY_PARQUET="$DW_ROOT"/activate/summary/rowkey_based/parquet/\{eu,ap,us\}

DW_ACTIVATE_SUMMARY_CHANNEL="$DW_ROOT"/activate_summary_channel/rowkey_based
DW_ACTIVATE_SUMMARY_CHANNEL_JSON="$DW_ROOT"/activate_summary_channel/rowkey_based/json
DW_ACTIVATE_SUMMARY_CHANNEL_PARQUET="$DW_ROOT"/activate_summary_channel/rowkey_based/parquet

DW_ACTIVATE_SOURCE_JSON="$DW_ROOT"/activate_source/smartinput/json
DW_ACTIVATE_SOURCE_PARQUET="$DW_ROOT"/activate_source/smartinput/parquet
DW_ACTIVATE_SOURCE_SUMMARY_JSON="$DW_ROOT"/activate_source_summary/smartinput/rowkey_based/json
DW_ACTIVATE_SOURCE_SUMMARY_PARQUET="$DW_ROOT"/activate_source_summary/smartinput/rowkey_based/parquet

DW_ACTIVATE_OEM_TO_ONLINE_JSON="$DW_ROOT"/activate_oem_to_online/smartinput/json
DW_ACTIVATE_OEM_TO_ONLINE_PARQUET="$DW_ROOT"/activate_oem_to_online/smartinput/parquet

DW_LAUNCH_ACTIVE_JSON="$DW_ROOT"/launch_active/smartinput/json/\{eu,ap,us\}
DW_LAUNCH_ACTIVE_PARQUET="$DW_ROOT"/launch_active/smartinput/parquet/\{eu,ap,us\}

DW_ACTIVATE_SOURCE_JSON="$DW_ROOT"/smartinput

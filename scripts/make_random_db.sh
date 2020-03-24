#!/usr/bin/env bash

WORK_DIR=work

cd $(dirname $0)/..
source queryenv

mkdir -p $WORK_DIR
cp testcases/* $WORK_DIR
cd $WORK_DIR
for table_name in BILL CART CARTDETAILS CUSTOMER; do
  # table size = range of PK, we max out the range fed to RandomDB.java so that
  # all FK constraints are automatically satisfied
  table_size=$(grep PK ${table_name}.det | awk '{ print $3 }')
  java RandomDB $table_name $table_size
  java ConvertTxtToTbl $table_name
  sed -i'' -e 's/\t$//' $table_name.txt # remove trailing \t for psql COPY
done

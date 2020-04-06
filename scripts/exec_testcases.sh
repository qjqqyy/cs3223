#!/usr/bin/env bash

WORK_DIR=work
PAGE_SIZE=1024
NUM_BUFFER=20

cd $(dirname $0)/..
source queryenv
cd $WORK_DIR

for i in query*.sql; do
  echo "===> RUNNING $i <==="
  java QueryMain $i ${i//.sql/}.txt $PAGE_SIZE $NUM_BUFFER
done

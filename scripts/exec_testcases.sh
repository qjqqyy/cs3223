#!/usr/bin/env bash

WORK_DIR=work
PAGE_SIZE=1024
NUM_BUFFER=20

source queryenv
cd $WORK_DIR

for i in query*.sql; do
  java QueryMain $i ${i//.sql/}.txt $PAGE_SIZE $NUM_BUFFER
done

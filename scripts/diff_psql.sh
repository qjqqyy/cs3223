#!/usr/bin/env bash

cd $(dirname $0)/..

docker run --rm -d -w /cs3223 -v $(pwd):/cs3223 -e POSTGRES_PASSWORD=aaa --name cs3223 postgres:12.2-alpine

sleep 10

docker exec cs3223 psql -U postgres -f '/cs3223/postgres/schema.sql'
docker exec cs3223 psql -U postgres -f '/cs3223/postgres/values.sql'

cd work
FAILED=0
for i in query*.txt; do
  echo "===> CHECKING $i <==="
  sed -e "/WHERE/s/,/ AND /g;s/\"/'/g" ${i//.txt/.sql} | \
    docker exec -i cs3223 psql -U postgres -f - --csv -A -F "	" -t -o "/cs3223/work/expected_$i"
  diff -u <(sed -e 's/\.[0-9]*/.truncated/' expected_$i | sort) <(sed -e '1d;s/\t$//;s/\.[0-9]*/.truncated/' $i | sort) || FAILED=1
done

docker stop cs3223
exit $FAILED

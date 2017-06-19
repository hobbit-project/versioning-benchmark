#!/bin/bash

MODE=off
ENDPOINT=http://localhost:9998/r43ples/sparql
QUERY=$1

curl -H "Accept: application/sparql-results+xml" --data "query_rewriting=$MODE&query= $QUERY" $ENDPOINTc

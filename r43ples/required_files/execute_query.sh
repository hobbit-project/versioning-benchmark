#!/bin/bash

MODE=off
ENDPOINT=http://localhost:9998/r43ples/sparql
QUERY=$1

curl -H "Accept: application/json" --data "query_rewriting=$MODE&query=$QUERY" $ENDPOINT

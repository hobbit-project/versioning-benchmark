#!/bin/bash

CURR_VERSION_FILE=$1
CW_TBD=$2
DESTINATION_PATH=$3

filename=$(basename "$CURR_VERSION_FILE")
filename="${filename%added.*}"
tbd=$(grep -F -f $CW_TBD $CURR_VERSION_FILE)
lines=$(echo "${tbd}" | wc -l)
if [[ "$lines" > "1" ]]; then
   printf "%s\n" "${tbd}" >> $DESTINATION_PATH/$filename"deleted.nt"
   echo $lines
fi

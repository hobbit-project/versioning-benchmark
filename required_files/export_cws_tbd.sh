#!/bin/bash

SOURCE_PATH=$1
CW_TBD=$2
DESTINATION_PATH=$3

for f in $SOURCE_PATH/generatedCreativeWorks*.added.nt; do
   filename=$(basename "$f")
   filename="${filename%added.*}"
   tbd=$(grep -F -f $CW_TBD $f)
   lines=$(echo "${tbd}" | wc -l)
   if [[ "$lines" > "1" ]]; then
      printf "%s\n" "${tbd}" >> $DESTINATION_PATH/$filename"deleted.nt"
      echo $lines
   fi
done

#!/bin/bash

CURR_VERSION=$1
CW_TBD=$2
DESTINATION_PATH=$3

export_deleted() {
   filename=$(basename "$1")
   filename="${filename%added.*}"
   tbd=$(grep -F -f $CW_TBD $1)
   lines=$(echo "${tbd}" | wc -l)
   if [[ "$lines" > "1" ]]; then
      printf "%s\n" "${tbd}" >> $DESTINATION_PATH/$filename"deleted.nt"
      echo $lines
   fi
}

# do it parallely in the background for improving performance
for ((i=0; i<$CURR_VERSION; i++)); do
   SOURCE_PATH=$([ "$i" = "0" ] && echo $4"/v"$i || echo $4"/c"$i)
   for f in $SOURCE_PATH/generatedCreativeWorks*.added.nt; do
      export_deleted $f &
   done
done
wait

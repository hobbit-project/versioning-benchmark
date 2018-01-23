#!/bin/bash

NUMBER_OF_VERSIONS=$1
DATASETS_PATH=$2

DATASETS_PATH_FINAL=$DATASETS_PATH/final

for ((i=0; i<$NUMBER_OF_VERSIONS; i++))
do
   if [ "$i" = "0" ]; then
      cp -r $DATASETS_PATH/v0 $DATASETS_PATH_FINAL
   else
      mkdir $DATASETS_PATH_FINAL/v$i
      prev=$((i-1))

      # dbpedia
      if ls $DATASETS_PATH/c$i/dbpedia_*.deleted.nt 1> /dev/null 2>&1; then
         # compute the old dbpedia triples that still exist
         for f in $DATASETS_PATH_FINAL/v$prev/dbpedia_*.added.nt; do
            comm_command="comm -23 $f "
            for ff in $DATASETS_PATH/c$i/dbpedia_*.deleted.nt; do
               comm_command+="$ff | comm -23 - "
            done
            filename=$(basename "$f")
            comm_command=${comm_command::-14}
            eval $comm_command > $DATASETS_PATH_FINAL/v$i/$filename &
         done
         wait
      else
         # copy the previous added
         cp $DATASETS_PATH_FINAL/v$prev/dbpedia_*.added.nt $DATASETS_PATH_FINAL/v$i
      fi
      # copy the current added
      cp $DATASETS_PATH/c$i/dbpedia_*.added.nt $DATASETS_PATH_FINAL/v$i

      # creative works
      if ls $DATASETS_PATH/c$i/generatedCreativeWorks-*.deleted.nt 1> /dev/null 2>&1; then
         # compute the old creative works that still exist
         for f in $DATASETS_PATH_FINAL/v$prev/generatedCreativeWorks*.added.nt; do
            comm_command="comm -23 $f "
            for ff in $DATASETS_PATH/c$i/generatedCreativeWorks*.deleted.nt; do
               comm_command+="$ff | comm -23 - "
            done
            filename=$(basename "$f")
            comm_command=${comm_command::-14}
            eval $comm_command > $DATASETS_PATH_FINAL/v$i/$filename &
         done
         wait
      else
         # copy the previous added
         cp $DATASETS_PATH_FINAL/v$prev/generatedCreativeWorks*.added.nt $DATASETS_PATH_FINAL/v$i
      fi
      # copy the current added
      cp $DATASETS_PATH/c$i/generatedCreativeWorks*.added.nt $DATASETS_PATH_FINAL/v$i
   fi
done

#!/bin/bash

VIRTUOSO_BIN=/usr/local/virtuoso-opensource/bin
GRAPH_NAME=http://graph.version.
DATASETS_PATH=/versioning/data
DATASETS_PATH_FINAL=/versioning/data/final
ONTOLOGIES_PATH=/versioning/ontologies
SERIALIZATION_FORMAT=$1
NUMBER_OF_VERSIONS=$2
total_cores=$(cat /proc/cpuinfo | grep processor | wc -l)
rdf_loaders=$(awk "BEGIN {printf \"%d\", $total_cores/2.5}")

echo "total cores: $total_cores"
prll_rdf_loader_run() {
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="set isolation='uncommitted';" > /dev/null
   for ((j=0; j<$1; j++)); do  
      $VIRTUOSO_BIN/isql-v 1111 dba dba exec="rdf_loader_run();" > /dev/null &
   done
   wait
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
   
   # if there are files that failed to be loaded reload them until the succeed
   errors=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select count(*) from load_list where ll_error is not null;" | sed -n 9p)
   files=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select ll_file from load_list where ll_error is not null;" | sed '1,8d' | head -n $errors)

   while [ "$errors" -gt "0" ]; do
      echo "The following "$errors" file(s) failed to be loaded. "
  	  echo $files
  	  echo "Retrying..."
	  $VIRTUOSO_BIN/isql-v 1111 dba dba exec="update load_list set ll_state = 0, ll_error = null where ll_error is not null;" > /dev/null
      for ((j=0; j<$1; j++)); do  
         $VIRTUOSO_BIN/isql-v 1111 dba dba exec="rdf_loader_run();" > /dev/null &
      done
      wait
      $VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
	  errors=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select count(*) from load_list where ll_error is not null;" | sed -n 9p)
   done
   
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="set isolation='committed';" > /dev/null
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="delete from load_list;" > /dev/null
   echo "All data files loaded successfully"
}

# prepare cw data files for loading
# sort files
start_sort=$(($(date +%s%N)/1000000))
for f in $(find $DATASETS_PATH -name 'generatedCreativeWorks-*.nt'); do 
   sort "$f" -o "$f"
done
end_sort=$(($(date +%s%N)/1000000))
sorttime=$(($end_sort - $start_sort))
echo "Sorted generated Creative Works in $sorttime ms."

# copy and compute the addsets 
start_prepare=$(($(date +%s%N)/1000000))
mkdir $DATASETS_PATH_FINAL
for ((i=0; i<$NUMBER_OF_VERSIONS; i++)); do
   echo "Constructing v$i..."
   if [ "$i" = "0" ]; then
      mkdir $DATASETS_PATH_FINAL/v$i
      cp $DATASETS_PATH/v$i/generatedCreativeWorks*.nt $DATASETS_PATH_FINAL/v$i
      cp $DATASETS_PATH/v$i/dbpedia_final/*.nt $DATASETS_PATH_FINAL/v$i
      cp $ONTOLOGIES_PATH/*.nt $DATASETS_PATH_FINAL/v$i
   else
      mkdir $DATASETS_PATH_FINAL/v$i
      cp $ONTOLOGIES_PATH/* $DATASETS_PATH_FINAL/v$i
      prev=$((i-1))

      # dbpedia
      # if current version contains dbpedia copy the dbpedia version, else copy the previous version
      if ls $DATASETS_PATH/c$i/dbpedia_final/dbpedia_*_1000_entities.nt 1> /dev/null 2>&1; then
        # copy the current version
        cp $DATASETS_PATH/c$i/dbpedia_final/dbpedia_*_1000_entities.nt $DATASETS_PATH_FINAL/v$i
      else
	 cp $DATASETS_PATH_FINAL/v$prev/dbpedia_*.nt $DATASETS_PATH_FINAL/v$i
      fi
      
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
   end_compute=$(($(date +%s%N)/1000000))

   # prepare bulk load
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="ld_dir('$DATASETS_PATH_FINAL/v$i', '*', '$GRAPH_NAME$i');" > /dev/null
done
end_prepare=$(($(date +%s%N)/1000000))

# bulk load
echo "Loading data files into virtuoso using $rdf_loaders rdf loaders..."
prll_rdf_loader_run $rdf_loaders
end_load=$(($(date +%s%N)/1000000))

for ((j=0; j<$NUMBER_OF_VERSIONS; j++)); do
   result=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="sparql select count(*) from <$GRAPH_NAME$j> where { ?s ?p ?o };" | sed -n 9p) > /dev/null
   echo $(echo $result | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta') "triples loaded to graph <"$GRAPH_NAME$j">"
done
end_size=$(($(date +%s%N)/1000000))

preptime=$(($end_prepare - $start_prepare))
loadingtime=$(($end_load - $end_prepare))
sizetime=$(($end_size - $end_load))
overalltime=$(($end_size - $start_sort))

echo "Loading of all generated data to Virtuoso triple store completed successfully. Time: $overalltime ms (preparation: $preptime, loading: $loadingtime, size: $sizetime)"


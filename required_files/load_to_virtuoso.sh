#!/bin/sh

VIRTUOSO_BIN=/opt/virtuoso-opensource/bin
GRAPH_NAME=http://graph.version.
DATASETS_PATH=/versioning/data/
ONTOLOGIES_PATH=/versioning/ontologies/
SERIALIZATION_FORMAT=$1
NUMBER_OF_VERSIONS=$2

for ((i=0; i<$NUMBER_OF_VERSIONS; i++)) do
   start_load=$(($(date +%s%N)/1000000))
   $VIRTUOSO_BIN/isql 1112 dba dba exec="delete from load_list;" > /dev/null
   $VIRTUOSO_BIN/isql 1112 dba dba exec="sparql clear GRAPH <$GRAPH_NAME$i>;" > /dev/null

   # load ontologies and triples of v0
   $VIRTUOSO_BIN/isql 1112 dba dba exec="ld_dir('$ONTOLOGIES_PATH', '*.nt', '$GRAPH_NAME$i');" > /dev/null
   $VIRTUOSO_BIN/isql 1112 dba dba exec="ld_dir('$DATASETS_PATH"v0"', '*.$SERIALIZATION_FORMAT', '$GRAPH_NAME$i');" > /dev/null

   # load triples of remaining change sets
   for ((j=1; j<=i; j++)) do  
      $VIRTUOSO_BIN/isql 1112 dba dba exec="ld_dir('$DATASETS_PATH"c"$j', '*.$SERIALIZATION_FORMAT', '$GRAPH_NAME$i');" > /dev/null
   done
   $VIRTUOSO_BIN/isql 1112 dba dba exec="set isolation='uncommitted';" > /dev/null

   # set the recommended number of rdf loaders (total number of cores / 2.5)
   total_cores=$(cat /proc/cpuinfo | grep processor | wc -l)
   rdf_loaders=$(awk "BEGIN {printf \"%d\", $total_cores/2.5}")
   for ((z=0; z<rdf_loaders; z++)) do  
      $VIRTUOSO_BIN/isql 1112 dba dba exec="rdf_loader_run();" > /dev/null &
   done
   wait

   $VIRTUOSO_BIN/isql 1112 dba dba exec="checkpoint;" > /dev/null
   end_load=$(($(date +%s%N)/1000000))

   # get the total size of loaded triples
   start_size=$(($(date +%s%N)/1000000))
   result=$($VIRTUOSO_BIN/isql 1112 dba dba exec="sparql select count(*) from <$GRAPH_NAME$i> where { ?s ?p ?o };" | sed -n 9p)
   $VIRTUOSO_BIN/isql 1112 dba dba exec="checkpoint;" > /dev/null
   end_size=$(($(date +%s%N)/1000000))

   loadingtime=$(($end_size - $start_load))
   sizetime=$(($end_size - $start_size))

   echo $(echo $result | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta') "triples loaded to graph <"$GRAPH_NAME$i">, using" $rdf_loaders "rdf loaders, for version v"$i". Time :" $loadingtime "ms"
done

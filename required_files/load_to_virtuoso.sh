#!/bin/sh

VIRTUOSO_BIN=/opt/virtuoso-opensource/bin
GRAPH_NAME=http://graph.version.
DATASETS_PATH=/versioning/data/
ONTOLOGIES_PATH=/versioning/ontologies/
SERIALIZATION_FORMAT=$1
NUMBER_OF_VERSIONS=$2
total_cores=$(cat /proc/cpuinfo | grep processor | wc -l)
rdf_loaders=$(awk "BEGIN {printf \"%d\", $total_cores/2.5}")
start_load=0
end_load=0
start_size=0
end_size=0
start_copy=0
end_copy=0
start_add=0
end_add=0
start_del=0
end_del=0

prll_rdf_loader_run() {
   $VIRTUOSO_BIN/isql 1112 dba dba exec="set isolation='uncommitted';" > /dev/null
   for ((j=0; j<$1; j++)) do  
      $VIRTUOSO_BIN/isql 1112 dba dba exec="rdf_loader_run();" > /dev/null &
   done
   wait
   $VIRTUOSO_BIN/isql 1112 dba dba exec="checkpoint;" > /dev/null
   $VIRTUOSO_BIN/isql 1112 dba dba exec="delete from load_list;" > /dev/null
   $VIRTUOSO_BIN/isql 1112 dba dba exec="set isolation='committed';" > /dev/null
}

start_load=$(($(date +%s%N)/1000000))
# load version 0
$VIRTUOSO_BIN/isql 1112 dba dba exec="ld_dir('$ONTOLOGIES_PATH', '*.ttl', '"$GRAPH_NAME"0');" > /dev/null
$VIRTUOSO_BIN/isql 1112 dba dba exec="ld_dir('"$DATASETS_PATH"v0', '*.added.$SERIALIZATION_FORMAT', '"$GRAPH_NAME"0');" > /dev/null  
prll_rdf_loader_run $rdf_loaders
end_load=$(($(date +%s%N)/1000000))

# load the remaining versions
for ((i=1; i<$NUMBER_OF_VERSIONS; i++)) do
   prev_version=$((i-1))

   start_load=$(($(date +%s%N)/1000000))

   # get the total size of loaded triples
   start_size=$(($(date +%s%N)/1000000))
   result=$($VIRTUOSO_BIN/isql 1112 dba dba exec="sparql select count(*) from <$GRAPH_NAME$prev_version> where { ?s ?p ?o };" | sed -n 9p) > /dev/null
   end_size=$(($(date +%s%N)/1000000))

   loadingtime=$(($end_size - $start_load))
   sizetime=$(($end_size - $start_size))
   copytime=$(($end_copy - $start_copy))
   addtime=$(($end_add - $start_add))
   deltime=$(($end_del - $start_del))

   echo $(echo $result | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta') "triples loaded to graph <"$GRAPH_NAME$prev_version">, using" $rdf_loaders "rdf loaders. Time : "$loadingtime" ms (size: "$sizetime", copy: "$copytime", add: "$addtime", del: "$deltime")"

   # copy triples of the previous version
   start_copy=$(($(date +%s%N)/1000000))
   $VIRTUOSO_BIN/isql 1112 dba dba exec="sparql insert { graph <$GRAPH_NAME$i> { ?s ?p ?o } } where { graph <$GRAPH_NAME$prev_version> { ?s ?p ?o } };" > /dev/null
   end_copy=$(($(date +%s%N)/1000000))

   # add the addsets
   start_add=$(($(date +%s%N)/1000000))
   $VIRTUOSO_BIN/isql 1112 dba dba exec="ld_dir('"$DATASETS_PATH"c"$i"', '*.added.$SERIALIZATION_FORMAT', '$GRAPH_NAME$i');" > /dev/null 
   prll_rdf_loader_run $rdf_loaders
   end_add=$(($(date +%s%N)/1000000))

   # delete the deletesets
   start_del=$(($(date +%s%N)/1000000))
   $VIRTUOSO_BIN/isql 1112 dba dba exec="ld_dir('"$DATASETS_PATH"c"$i"', '*.deleted.$SERIALIZATION_FORMAT', '$GRAPH_NAME$i.deleted');" > /dev/null
   prll_rdf_loader_run $rdf_loaders
   $VIRTUOSO_BIN/isql 1112 dba dba exec="sparql delete { graph <$GRAPH_NAME$i> { ?s ?p ?o} } where { graph <$GRAPH_NAME$i.deleted> { ?s ?p ?o } };" > /dev/null
   $VIRTUOSO_BIN/isql 1112 dba dba exec="sparql drop silent graph <$GRAPH_NAME$i.deleted>;" > /dev/null
   end_del=$(($(date +%s%N)/1000000))
done

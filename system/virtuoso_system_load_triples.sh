#!/bin/bash

VIRTUOSO_BIN=/usr/local/virtuoso-opensource/bin
GRAPH_NAME=http://graph.version.
DATASETS_PATH=/versioning/data/
ONTOLOGIES_PATH=/versioning/ontologies/
SERIALIZATION_FORMAT=$1
VERSION_NUMBER=$2

<<COMMENT1
# wait until loading tasks of previous versions have completed
previous_version=$((VERSION_NUMBER-1))
echo "previous version: "$previous_version

while true 
do
   echo "mpike sto while true"
   if [ $previous_version -lt 0 ]; then
      echo "itan to prwto version, ekane break"
      break
   fi

   echo "itan epomeno version"
   previous_existing_tasks=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select count(*) from load_list where ll_graph = 'http://graph.version."$previous_version"';" | sed -n 9p)
   previous_finished_tasks=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select count(*) from load_list where ll_state = 2 and ll_graph = 'http://graph.version."$previous_version"';" | sed -n 9p)

   if [ "$revious_existing_tasks" = "$previous_finished_tasks" ]; then 
      echo "DEN einai idia: previous_existing_tasks: "$previous_existing_tasks" previous_finished_tasks: "$previous_finished_tasks
      sleep 1
      previous_finished_tasks=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="select count(*) from load_list where ll_state = 2 and ll_graph = 'http://graph.version."$previous_version"';" | sed -n 9p)
   else
      echo "EINAI idia: previous_existing_tasks: "$previous_existing_tasks" previous_finished_tasks: "$previous_finished_tasks", ekane break"
      break
   fi
done
COMMENT1

# set the recommended number of rdf loaders (total number of cores / 2.5)
total_cores=$(cat /proc/cpuinfo | grep processor | wc -l)
rdf_loaders=$(awk "BEGIN {printf \"%d\", $total_cores/2.5}")

start_load=$(($(date +%s%N)/1000000))
$VIRTUOSO_BIN/isql-v 1111 dba dba exec="delete from load_list;" > /dev/null
$VIRTUOSO_BIN/isql-v 1111 dba dba exec="sparql clear GRAPH <$GRAPH_NAME$VERSION_NUMBER>;" > /dev/null

# load ontologies and triples of version 0
$VIRTUOSO_BIN/isql-v 1111 dba dba exec="ld_dir('$ONTOLOGIES_PATH', '*.ttl', '$GRAPH_NAME$VERSION_NUMBER');" > /dev/null
$VIRTUOSO_BIN/isql-v 1111 dba dba exec="ld_dir('$DATASETS_PATH"v0"', '*.$SERIALIZATION_FORMAT', '$GRAPH_NAME$VERSION_NUMBER');" > /dev/null

for ((i=1; i<=$VERSION_NUMBER; i++)) do
   # load triples of change sets
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="ld_dir('$DATASETS_PATH"c"$i', '*.$SERIALIZATION_FORMAT', '$GRAPH_NAME$VERSION_NUMBER');" > /dev/null
done

$VIRTUOSO_BIN/isql-v 1111 dba dba exec="set isolation='uncommitted';" > /dev/null

for ((z=0; z<$rdf_loaders; z++)) do  
   $VIRTUOSO_BIN/isql-v 1111 dba dba exec="rdf_loader_run();" > /dev/null &
done
wait

$VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
end_load=$(($(date +%s%N)/1000000))
loadingtime=$(($end_load - $start_load))

# get the total size of loaded triples
start_size=$(($(date +%s%N)/1000000))
result=$($VIRTUOSO_BIN/isql-v 1111 dba dba exec="sparql select count(*) from <$GRAPH_NAME$VERSION_NUMBER> where { ?s ?p ?o };" | sed -n 9p)
$VIRTUOSO_BIN/isql-v 1111 dba dba exec="checkpoint;" > /dev/null
end_size=$(($(date +%s%N)/1000000))
sizetime=$(($end_size - $start_size))

#
echo "triples:"$result",time:"$loadingtime

echo $(echo $result | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta') "triples loaded to graph <"$GRAPH_NAME$VERSION_NUMBER">, using" $rdf_loaders "rdf loaders, for version v"$VERSION_NUMBER". Time :" $loadingtime "ms"


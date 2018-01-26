#!/bin/sh

# set the recommended number of rdf loaders (total number of cores / 2.5)
TOTAL_CORES=$(cat /proc/cpuinfo | grep processor | wc -l)
NUMBER_OF_LOADERS=$(awk "BEGIN {printf \"%d\", $TOTAL_CORES/2.5}")

#!/bin/bash
ADDRESS=$1
PORT=1111
FOLDER=$2
GRAPHURI=$3

sleep 1
start_load=$(($(date +%s%N)/1000000))
echo "delete from load_list;" | isql $ADDRESS:$PORT
isql $ADDRESS:$PORT exec="DB.DBA.RDF_OBJ_FT_RULE_DEL (null, null, 'ALL');;"
echo "ld_dir('"$FOLDER"', '*', '"$GRAPHURI"');" | isql $ADDRESS:$PORT
for i in `seq 1 $NUMBER_OF_LOADERS`;
do
    isql $ADDRESS:$PORT exec="rdf_loader_run()" &
done
wait

echo "checkpoint;" | isql $ADDRESS:$PORT
end_load=$(($(date +%s%N)/1000000))
loadingtime=$(($end_load - $start_load))

# logging
echo "All data loaded to graph <"$GRAPHURI">, using "$NUMBER_OF_LOADERS" rdf loaders. Time : "$loadingtime" ms"


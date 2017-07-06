#!/bin/bash

JAR=r43ples-console-client.jar
CONFIG=conf/r43ples.tdb.conf
#CONFIG=conf/r43ples.stardog.conf
#CONFIG=conf/r43ples.virtuoso.conf


VERSION_NUMBER=$1
GRAPH=http://test.com/r43ples

# Create initial graph
if [ $VERSION_NUMBER = 0 ]; then
    echo $(date +%H:%M:%S.%N | cut -b1-12)" : Creating initial graph '"$GRAPH"'..."
    java -jar $JAR --config $CONFIG --new --graph $GRAPH

    echo $(date +%H:%M:%S.%N | cut -b1-12)" : Loading triples of v0 to graph '"$GRAPH"'..."
    java -jar $JAR --config $CONFIG -g $GRAPH -a /versioning/toLoad/initial-version.nt -m 'add initial version'
else
    echo $(date +%H:%M:%S.%N | cut -b1-12)" : Loading triples of v"$VERSION_NUMBER" to graph '"$GRAPH"'..."
    java -jar $JAR --config $CONFIG -g $GRAPH -a /versioning/toLoad/changeset-add-$VERSION_NUMBER.nt -m 'load version '$VERSION_NUMBER
fi

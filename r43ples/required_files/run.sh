#!/bin/sh

# Start R43ples
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Starting R43ples..."
java -jar r43ples.jar --config conf/r43ples.tdb.conf &     	# TDB     -> 9998
#java -jar r43ples.jar --config conf/r43ples.stardog.conf &	# Stardog -> 9997
#java -jar r43ples.jar --config conf/r43ples.virtuoso.conf &	# Virtuoso -> 9996

# run the system adapter
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Running the System adapter..."
java -cp /versioning.jar org.hobbit.core.run.ComponentStarter org.hobbit.benchmark.versioning.systems.R43plesSystemAdapter

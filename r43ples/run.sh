#!/bin/sh

# Start R43ples
java -jar r43ples.jar --config conf/r43ples.tdb.conf &     # TDB     -> 9998

# run the system adapter
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Running the System adapter..."
java -cp versioning.jar org.hobbit.core.run.ComponentStarter org.hobbit.benchmark.versioning.systems.R43plesSystemAdapter

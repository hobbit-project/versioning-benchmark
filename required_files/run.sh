#!/bin/sh

rm virtuoso_run.log 2> /dev/null

echo $(date +%H:%M:%S.%N | cut -b1-12)" : Starting OpenLink Virtuoso Universal Server..."
/opt/virtuoso-opensource/bin/virtuoso-t -f > /versioning/virtuoso_run.log 2>&1 &
seconds_passed=0

# wait until virtuoso is ready
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Waiting until Virtuoso Server is online..."
until grep -m 1 "Server online at 1112" /versioning/virtuoso_run.log
do
  sleep 1
  seconds_passed=$((seconds_passed+1))
  echo $seconds_passed >> out.txt
  if [ $seconds_passed -gt 120 ]; then
    echo $(date +%H:%M:%S.%N | cut -b1-12)" : Could not start Virtuoso Server. Timeout: [2 min]"
    break
  fi
done
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Virtuoso Server started successfully."

# run the data generator
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Running the Data Generator..."
java -cp /versioning/versioning.jar org.hobbit.core.run.ComponentStarter org.hobbit.benchmark.versioning.components.VersioningDataGenerator

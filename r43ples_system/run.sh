#!/bin/sh

rm /r43ples/r43ples_server.log 2> /dev/null

# Start R43ples Server
echo "" > /r43ples/r43ples_server.log 2>&1
echo "Starting R43ples Server..."
java -jar /r43ples/target/r43ples-0.8.8-jar-with-dependencies.jar -c /r43ples/conf/r43ples.tdb.conf > /r43ples/r43ples_server.log 2>&1 &
seconds_passed=0

# wait until r43ples is ready
until grep -m 1 "Server started - R43ples endpoint available under" /r43ples/r43ples_server.log
do
   sleep 1
   seconds_passed=$((seconds_passed+1))
   echo $seconds_passed >> out.txt
   if [ $seconds_passed -gt 120 ]; then
      echo "Could not start R43ples Server. Timeout: [2 min]"
      echo "Exiting..."
      exit
   fi
done

# run the system adapter
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Running the System adapter..."
java -cp /versioning.jar org.hobbit.core.run.ComponentStarter org.hobbit.benchmark.versioning.systems.R43plesSystemAdapter


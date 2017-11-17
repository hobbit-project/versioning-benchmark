#!/bin/sh

rm virtuoso_run.log 2> /dev/null
cp virtuoso.ini.template virtuoso.ini

# adjust virtuoso memory usage settings
free_memory=$(free | awk 'NR==2 {print $4}')
NumberOfBuffers=$(($free_memory*660/8000))
MaxDirtyBuffers=$(($NumberOfBuffers*3/4))
sed -i -e "s/NumberOfBuffers          = 10000/NumberOfBuffers          = $NumberOfBuffers/g" virtuoso.ini
sed -i -e "s/MaxDirtyBuffers          = 6000/MaxDirtyBuffers          = $MaxDirtyBuffers/g" virtuoso.ini
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Virtuoso memory usage adjusted to the available resources:"
echo $(date +%H:%M:%S.%N | cut -b1-12)" : NumberOfBuffers="$NumberOfBuffers
echo $(date +%H:%M:%S.%N | cut -b1-12)" : MaxDirtyBuffers="$MaxDirtyBuffers

echo $(date +%H:%M:%S.%N | cut -b1-12)" : Starting OpenLink Virtuoso Universal Server..."
virtuoso-t -f > /versioning/virtuoso_run.log 2>&1 &
seconds_passed=0

# wait until virtuoso is ready
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Waiting until Virtuoso Server is online..."
until grep -m 1 "Server online at 1111" /versioning/virtuoso_run.log
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

# run the system adapter
echo $(date +%H:%M:%S.%N | cut -b1-12)" : Running the System adapter..."
java -cp /versioning/versioning.jar org.hobbit.core.run.ComponentStarter org.hobbit.benchmark.versioning.systems.VirtuosoSystemAdapter

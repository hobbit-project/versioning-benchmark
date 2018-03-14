#!/bin/bash

ps -ef | grep r43ples.jar | grep -v grep | awk '{print $2}' | xargs kill
java -jar r43ples.jar --config conf/r43ples.tdb.conf &

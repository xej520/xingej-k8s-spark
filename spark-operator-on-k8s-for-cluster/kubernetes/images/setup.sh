#!/bin/bash

if [ $ROLE = "master" ]; then
	echo "----start---master---"
	$SPARK_HOME/sbin/start-master.sh
elif [ $ROLE = "slave" ]; then
	echo "----start---slave----"
        $SPARK_HOME/sbin/start-slave.sh $MASTER:7077
fi


while true
do
	sleep 5
done

#!/bin/bash
for i in `seq 1 100`
do
	{	
		sleep 2
		echo $i
	}&

    if (( $i % 4 == 0 ))
    then
        echo "wait"
        wait
    fi
done

#!/bin/bash
set -x

dates=$(./precondition -D -d daaprawcdwdata -dp cdw_viewership_reports)

results=($dates)

if [ ${results[0]} != "true" ]; then
	echo "Could not find dates:"
	echo "$dates" 
	exit -1
fi

from=${results[1]}
to=${results[2]}

echo $from, $to


dateFrom=$(date -d "$from" +%s)
dateTo=$(date -d "$to" +%s)

if [ $dateFrom -gt $dateTo ]; then
        echo "No need to run"
        exit 0
else
    	echo "running"
fi

./run.sh $from $to

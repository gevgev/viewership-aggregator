#!/bin/bash
set -x

if [ "$#" -ne 1 ]; then
	echo "Error: Missing parameters:"
	echo "  days <2/3>"
	exit 1
fi

days=$1

# last raw downloaded to daap date from cdw
dateRaw=$(./precondition -D -d daaprawcdwdata -dp cdw_viewership_reports)

results=($dateRaw)

if [ ${results[0]} != "true" ]; then
	echo "Could not find dates:"
	echo "$dates" 
	exit -1
fi

to=${results[1]}

# last daap aggregated (hh) report generated date
dateAggr=$(./precondition -D -d daapreports -dp hh_count3d)

results=($dateAggr)

if [ ${results[0]} != "true" ]; then
	echo "Could not find dates:"
	echo "$dates" 
	exit -1
fi

from=${results[1]}

echo $from, $to


dateFrom=$(date -d "$from" +%s)
dateTo=$(date -d "$to" +%s)

#go to next day after the last aggregated report
dateFrom=$(date -I -d "$from + 1 day")

if [ $dateFrom -gt $dateTo ]; then
        echo "No need to run"
        exit 0
else
    	echo "running"
fi

./run.sh $from $to $days

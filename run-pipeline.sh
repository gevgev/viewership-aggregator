#!/bin/bash
set -x

if [ "$#" -ne 1 ]; then
	echo "Error: Missing parameters:"
	echo "  days <2/3>"
	exit 1
fi

days=$1

if [ "$days" == "3" ]; then
  hh_count="hh_count3d"
else
  hh_count="hh_count2d"
fi

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
dateAggr=$(./precondition -D -d daapreports -dp "$hh_count")

results=($dateAggr)

if [ ${results[0]} != "true" ]; then
	echo "Could not find dates:"
	echo "$dates" 
	exit -1
fi

from=${results[1]}

echo $from, $to

# adjust the date to reflect the days after the last aggregated report
from=$(date -I -d "$from + $days day")

# go to next day after the last aggregated report
from=$(date -I -d "$from - 1 day")

dateFrom=$(date -d "$from" +%s)
dateTo=$(date -d "$to" +%s)

if [ $dateFrom -gt $dateTo ]; then
        echo "No need to run"
        exit 0
else
    	echo "running"
fi

./run.sh $from $to $days

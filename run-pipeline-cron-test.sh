#!/bin/bash
set -x

if [ "$#" -ne 1 ]; then
	echo "Error: Missing parameters:"
	echo "  days <2/3>"
	exit 1
fi

days=$1
from="2016-08-01" 
to="2016-08-05"

./run.sh $from $to $days

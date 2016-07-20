#!/bin/bash
set -x

dates=$(./precondition -D)

results=($dates)

if [ ${results[0]} != "true" ]; then
	echo "Could not find dates:"
	echo "$dates" 
	exit -1
fi

from=${results[1]}
to=${results[2]}

echo $from, $to

./run.sh $from $to

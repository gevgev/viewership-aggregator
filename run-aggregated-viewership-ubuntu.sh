#!/bin/sh

if [ "$#" -ne 2 ]; then
	echo "Error: Missing parameters:"
	echo "  from <20160701"
	echo "  to <20160709>"
	exit 1
fi

set -x


from=$1
to=$2

./viewership-aggregator -from "$from" -to "$to"

find cdw-viewership-reports -type f -exec rm {} +

d="$from"
up=$(date -I -d "$to + 1 day")

while [ "$d" != "$up" ]; do 
  dd=$(date "$d" +%Y%m%d)
  mv viewership-report-"$dd".csv cdw-viewership-reports/"$dd"/
  d=$(date -I -d "$d + 1 day")
done

FILES="cdw-viewership-reports/*/*.csv"
# get the latest file in the latest subdirectory for that provider
for file in $FILES
    do  
    echo "Compressing $file"
    gzip "$file"
done

aws s3 cp ./cdw-viewership-reports/ s3://daap-viewership-reports/cdw-viewership-reports/ --recursive

#!/bin/sh

if [ "$#" -ne 2 ]; then
	echo "Error: Missing parameters:"
	echo "  from <2016-07-01"
	echo "  to <2016-07-09>"
	exit 1
fi

set -x


from=$1
to=$2

./viewership-aggregator -from "$from" -to "$to"

find cdw-viewership-reports -type f -exec rm {} +

d="$from"
up=$(date -j -v +1d -f %Y-%m-%d "$to" +%Y-%m-%d)

while [ "$d" != "$up" ]; do 
  dd=$(date -j -v +0d -f %Y-%m-%d "$d" +%Y%m%d)
  mv viewership-report-"$dd".csv cdw-viewership-reports/"$dd"/
  d=$(date -j -v +1d -f %Y-%m-%d "$d" +%Y-%m-%d)
done

FILES="cdw-viewership-reports/*/*.csv"
# get the latest file in the latest subdirectory for that provider
for file in $FILES
    do  
    echo "Compressing $file"
    gzip "$file"
done

aws s3 cp ./cdw-viewership-reports/ s3://daap-viewership-reports/cdw-viewership-reports/ --recursive

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

mv cdw_viewership_reports viewership2d

# deleting input data, preserving folder structure
find viewership2d -type f -exec rm {} +

d="$from"
up=$(date -j -v +1d -f %Y-%m-%d "$to" +%Y-%m-%d)

# moving reports to under date folders
while [ "$d" != "$up" ]; do 
  dd=$(date -j -v +0d -f %Y-%m-%d "$d" +%Y%m%d)
  mv aggregated_viewership_"$dd".csv viewership2d/"$dd"/
  d=$(date -j -v +1d -f %Y-%m-%d "$d" +%Y-%m-%d)
done

# compressing files
FILES="viewership2d/*/*.csv"
# get the latest file in the latest subdirectory for that provider
for file in $FILES
    do  
    echo "Compressing $file"
    gzip "$file"
done

# uploading viewership files to AWS
aws s3 cp ./viewership2d/ s3://daapreports/viewership2d/ --recursive

mv viewership2d hh_count
# deleting viewership files, preserving folder structure
find hh_count -type f -exec rm {} +

d="$from"
up=$(date -j -v +1d -f %Y-%m-%d "$to" +%Y-%m-%d)

# moving hh_count reports
while [ "$d" != "$up" ]; do 
  dd=$(date -j -v +0d -f %Y-%m-%d "$d" +%Y%m%d)

  mv hh_count_*"$dd".csv hh_count/"$dd"/
  d=$(date -j -v +1d -f %Y-%m-%d "$d" +%Y-%m-%d)
done

# uploading viewership files to AWS
aws s3 cp ./hh_count/ s3://daapreports/hh_count/ --recursive

# clean up
rm -fR hh_count

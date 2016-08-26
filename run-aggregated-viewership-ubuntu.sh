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

mv cdw_viewership_reports viewership3d

# deleting input data, preserving folder structure
find viewership3d -type f -exec rm {} +

d="$from"
up=$(date -I -d "$to + 1 day")

# moving reports to under date folders
while [ "$d" != "$up" ]; do 
  dd=$(date -d "$d" +%Y%m%d)
  mv aggregated_viewership_"$dd".csv viewership3d/"$dd"/
  d=$(date -I -d "$d + 1 day")
done

# compressing files
FILES="viewership3d/*/*.csv"
# get the latest file in the latest subdirectory for that provider
for file in $FILES
    do  
    echo "Compressing $file"
    gzip "$file"
done

# uploading viewership files to AWS
aws s3 cp ./viewership3d/ s3://daapreports/viewership3d/ --recursive

mv viewership3d hh_count3d
# deleting viewership files, preserving folder structure
find hh_count3d -type f -exec rm {} +

d="$from"
up=$(date -I -d "$to + 1 day")

# moving hh_count reports
while [ "$d" != "$up" ]; do 
  dd=$(date -d "$d" +%Y%m%d)

  mv hh_count_*"$dd".csv hh_count3d/"$dd"/
  d=$(date -I -d "$d + 1 day")
done

# uploading viewership files to AWS
aws s3 cp ./hh_count3d/ s3://daapreports/hh_count3d/ --recursive

# clean up
rm -fR hh_count3d

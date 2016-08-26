#!/bin/sh

if [ "$#" -ne 3 ]; then
	echo "Error: Missing parameters:"
	echo "  from <2016-07-01"
	echo "  to <2016-07-09>"
  echo "  days <2/3>"
	exit 1
fi

set -x


from=$1
to=$2
days=$3-1

if ["$days" == "2"]; then
  viewership="viewership3d"
  hh_count="hh_count3d"
else
  viewership="viewership2d"
  hh_count="hh_count2d"
fi

./viewership-aggregator -from "$from" -to "$to" -d "$days"

mv cdw_viewership_reports "$viewership"

# deleting input data, preserving folder structure
find "$viewership" -type f -exec rm {} +

d="$from"
up=$(date -I -d "$to + 1 day")

# moving reports to under date folders
while [ "$d" != "$up" ]; do 
  dd=$(date -d "$d" +%Y%m%d)
  mv aggregated_viewership_"$dd".csv "$viewership"/"$dd"/
  d=$(date -I -d "$d + 1 day")
done

# compressing files
FILES="$viewership"/*/*.csv
# get the latest file in the latest subdirectory for that provider
for file in $FILES
    do  
    echo "Compressing $file"
    gzip "$file"
done

# uploading viewership files to AWS
aws s3 cp ./"$viewership"/ s3://daapreports/"$viewership"/ --recursive

mv "$viewership" "$hh_count"
# deleting viewership files, preserving folder structure
find "$hh_count" -type f -exec rm {} +

d="$from"
up=$(date -I -d "$to + 1 day")

# moving hh_count reports
while [ "$d" != "$up" ]; do 
  dd=$(date -d "$d" +%Y%m%d)

  mv hh_count_*"$dd".csv "$hh_count"/"$dd"/
  d=$(date -I -d "$d + 1 day")
done

# uploading viewership files to AWS
aws s3 cp ./"$hh_count"/ s3://daapreports/"$hh_count"/ --recursive

# clean up
rm -fR "$hh_count"

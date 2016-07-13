#!/bin/sh
set -x

mkdir build-ec2

cd build-ec2/

echo "Build viewership-aggregator"
GOOS=linux go build -v github.com/gevgev/viewership-aggregator

rc=$?; if [[ $rc != 0 ]]; then 
	echo "Build failed: viewership-aggregator"
	cd ..
	exit $rc; 
fi


echo "Copying script and mso list"
cp ../run-aggregated-viewership-ubuntu.sh run.sh
cp ../mso-list-full.csv mso-list.csv

echo "Archiving"

zip archive.zip *

echo 'Success'
cd ..
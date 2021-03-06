#!/bin/sh
set -x

mkdir build-data-pipeline

cd build-data-pipeline

echo "Build viewership-aggregator"
GOOS=linux go build -v github.com/gevgev/viewership-aggregator

rc=$?; if [[ $rc != 0 ]]; then 
	echo "Build failed: viewership-aggregator"
	cd ..
	exit $rc; 
fi

echo "Build precondition"
GOOS=linux go build -v github.com/gevgev/precondition

rc=$?; if [[ $rc != 0 ]]; then 
	echo "Build failed: precondition"
	cd ..
	exit $rc; 
fi

echo "Copying script and mso list"
cp ../run-aggregated-viewership-ubuntu-test-cron.sh run.sh
cp ../mso-list-full.csv mso-list.csv
cp ../run-pipeline-cron-test.sh loop.sh

chmod u+x ./run.sh
chmod u+x ./loop.sh

echo "Pushing to S3"

aws s3 cp . s3://daap-pipeline/viewership-aggregator-test --recursive

echo "Archiving"

zip archive-aggregator.zip *

echo 'Success'
cd ..
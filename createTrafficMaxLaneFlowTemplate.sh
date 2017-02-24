#!/bin/bash

project=$(gcloud info | egrep ^Project | tr -d '[]' | awk '{ print $2 }')
stagingbucket=gs://$project-staging
gsutil ls $stagingbucket >/dev/null || gsutil mb $stagingbucket

mvn compile exec:java \
	-Dexec.mainClass=com.example.dataflow.TrafficMaxLaneFlow \
	-Dexec.args="--project=$project --numWorkers=1 --maxNumWorkers=4 --autoscalingAlgorithm=THROUGHPUT_BASED --streaming=true --pubsubTopic=projects/$project/topics/traffic-topic --inputFile='gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv' --templateLocation=$stagingbucket/TrafficMaxLaneFlow --runner=DataflowRunner"

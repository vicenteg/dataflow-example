#!/bin/bash

project=$(gcloud info | egrep ^Project | tr -d '[]' | awk '{ print $2 }')
mvn compile exec:java \
	-Dexec.mainClass=com.example.dataflow.TrafficMaxLaneFlow \
	-Dexec.args="--project=$project --numWorkers=1 --maxNumWorkers=4 --autoscalingAlgorithm=THROUGHPUT_BASED --streaming=true --pubsubTopic=projects/$project/topics/traffic-topic --runner=DataflowRunner"

#!/bin/bash

project=$(gcloud info | egrep ^Project | tr -d '[]' | awk '{ print $2 }')
mvn compile exec:java \
	-Dexec.mainClass=com.example.dataflow.PubsubFileInjector \
	-Dexec.args="--project=$project --outputTopic=projects/$project/topics/traffic-topic --input=gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv --runner=DataflowRunner"

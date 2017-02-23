#!/bin/sh

project=$(gcloud info | egrep ^Project | tr -d '[]' | awk '{ print $2 }')
stagingbucket=gs://$project-staging

gcloud beta dataflow jobs run traffic-max-lane-flow \
	--gcs-location=$stagingbucket/TrafficMaxLaneFlow \
	--parameters="streaming=true pubsubTopic=projects/$project/topics/traffic-topic inputFile='gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv'"

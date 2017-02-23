#!/bin/sh

project=$(gcloud info | egrep ^Project | tr -d '[]' | awk '{ print $2 }')
stagingbucket=gs://$project-staging

gcloud beta dataflow jobs run pubsub-file-injector \
	--gcs-location=$stagingbucket/PubsubFileInjector \
	--parameters="outputTopic=projects/$project/topics/traffic-topic input='gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv'"

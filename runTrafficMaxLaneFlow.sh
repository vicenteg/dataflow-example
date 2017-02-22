#!/bin/bash

mvn compile exec:java \
	-Dexec.mainClass=com.example.dataflow.TrafficMaxLaneFlow \
	-Dexec.args="--streaming=true --pubsubTopic=projects/df-workshop-159315/topics/traffic-topic --inputFile='gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv'"

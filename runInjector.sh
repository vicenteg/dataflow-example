#!/bin/bash

mvn compile exec:java \
	-Dexec.mainClass=com.example.dataflow.PubsubFileInjector \
	-Dexec.args="--outputTopic=projects/df-workshop-159315/topics/traffic-topic --input=gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv --runner=DataflowRunner"

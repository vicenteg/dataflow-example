/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.example.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.avro.reflect.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Dataflow Example that runs in both batch and streaming modes with traffic sensor data. You can
 * configure the running mode by setting {@literal --streaming} to true or false.
 *
 * <p>Concepts: The batch and streaming runners, sliding windows, Google Cloud Pub/Sub topic
 * injection, use of the AvroCoder to encode a custom class, and custom Combine transforms.
 *
 * <p>This example analyzes traffic sensor data using SlidingWindows. For each window, it finds the
 * lane that had the highest flow recorded, for each sensor station. It writes those max values
 * along with auxiliary info to a BigQuery table.
 *
 * <p>In batch mode, the pipeline reads traffic sensor data from {@literal --inputFile}.
 *
 * <p>In streaming mode, the pipeline reads the data from a Pub/Sub topic. By default, the example
 * will run a separate pipeline to inject the data from the default {@literal --inputFile} to the
 * Pub/Sub {@literal --pubsubTopic}. It will make it available for the streaming pipeline to
 * process. You may override the default {@literal --inputFile} with the file of your choosing. You
 * may also set {@literal --inputFile} to an empty string, which will disable the automatic Pub/Sub
 * injection, and allow you to use separate tool to control the input to this example. An example
 * code, which publishes traffic sensor data to a Pub/Sub topic, is provided in <a
 * href="https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/tree/master/gce-cmdline-publisher"></a>.
 *
 * <p>The example is configured to use the default Pub/Sub topic and the default BigQuery table from
 * the example common package (there are no defaults for a general Dataflow pipeline). You can
 * override them by using the {@literal --pubsubTopic}, {@literal --bigQueryDataset}, and {@literal
 * --bigQueryTable} options. If the Pub/Sub topic or the BigQuery table do not exist, the example
 * will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class TrafficMaxLaneFlow {

  static final int WINDOW_DURATION = 60; // Default sliding window duration in minutes
  static final int WINDOW_SLIDE_EVERY = 1; // Default window 'slide every' setting in minutes

  /**
   * This class holds information about each lane in a station reading, along with some general
   * information from the reading.
   */
  @DefaultCoder(AvroCoder.class)
  static class LaneInfo {
    @Nullable String stationId;
    @Nullable String lane;
    @Nullable String direction;
    @Nullable String freeway;
    @Nullable String recordedTimestamp;
    @Nullable Integer laneFlow;
    @Nullable Integer totalFlow;
    @Nullable Double laneAO;
    @Nullable Double laneAS;

    LaneInfo() {}

    LaneInfo(
        String stationId,
        String lane,
        String direction,
        String freeway,
        String timestamp,
        Integer laneFlow,
        Double laneAO,
        Double laneAS,
        Integer totalFlow) {
      this.stationId = stationId;
      this.lane = lane;
      this.direction = direction;
      this.freeway = freeway;
      this.recordedTimestamp = timestamp;
      this.laneFlow = laneFlow;
      this.laneAO = laneAO;
      this.laneAS = laneAS;
      this.totalFlow = totalFlow;
    }

    public String getStationId() {
      return this.stationId;
    }

    String getLane() {
      return this.lane;
    }

    String getDirection() {
      return this.direction;
    }

    String getFreeway() {
      return this.freeway;
    }

    String getRecordedTimestamp() {
      return this.recordedTimestamp;
    }

     Integer getLaneFlow() {
      return this.laneFlow;
    }

     Double getLaneAO() {
      return this.laneAO;
    }

     Double getLaneAS() {
      return this.laneAS;
    }

     Integer getTotalFlow() {
      return this.totalFlow;
    }
  }

  /**
   * Extract flow information for each of the 8 lanes in a reading, and output as separate tuples.
   * This will let us determine which lane has the max flow for that station over the span of the
   * window, and output not only the max flow from that calculation, but other associated
   * information. The number of lanes for which data is present depends upon which freeway the data
   * point comes from.
   */
  static class ExtractFlowInfoFn extends DoFn<String, KV<String, LaneInfo>> {
    private static final long serialVersionUID = 0;
    private static final DateTimeFormatter dateTimeFormat =
        DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");

    private final boolean outputTimestamp;

     ExtractFlowInfoFn(boolean outputTimestamp) {
      this.outputTimestamp = outputTimestamp;
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext c) {
      String[] items = c.element().split(",");
      if (items.length < 48) {
        // Skip the invalid input.
        return;
      }
      // extract the sensor information for the lanes from the input string fields.
      String timestamp = items[0];
      String stationId = items[1];
      String freeway = items[2];
      String direction = items[3];
      Integer totalFlow = tryIntParse(items[7]);
      for (int i = 1; i <= 8; ++i) {
        Integer laneFlow = tryIntParse(items[6 + 5 * i]);
        Double laneAvgOccupancy = tryDoubleParse(items[7 + 5 * i]);
        Double laneAvgSpeed = tryDoubleParse(items[8 + 5 * i]);
        if (laneFlow == null || laneAvgOccupancy == null || laneAvgSpeed == null) {
          return;
        }
        LaneInfo laneInfo =
            new LaneInfo(
                stationId,
                "lane" + i,
                direction,
                freeway,
                timestamp,
                laneFlow,
                laneAvgOccupancy,
                laneAvgSpeed,
                totalFlow);
        if (outputTimestamp) {
          try {
            c.outputWithTimestamp(
                KV.of(stationId, laneInfo), new Instant(dateTimeFormat.parseMillis(timestamp)));
          } catch (IllegalArgumentException e) {
            // Skip the invalid input.
          }
        } else {
          c.output(KV.of(stationId, laneInfo));
        }
      }
    }
  }

  /**
   * A custom 'combine function' used with the Combine.perKey transform. Used to find the max lane
   * flow over all the data points in the Window. Extracts the lane flow from the input string and
   * determines whether it's the max seen so far. We're using a custom combiner instead of the Max
   * transform because we want to retain the additional information we've associated with the flow
   * value.
   */
  public static class MaxFlow implements SerializableFunction<Iterable<LaneInfo>, LaneInfo> {
    private static final long serialVersionUID = 0;

    @Override
    public LaneInfo apply(Iterable<LaneInfo> input) {
      Integer max = 0;
      LaneInfo maxInfo = new LaneInfo();
      for (LaneInfo item : input) {
        Integer flow = item.getLaneFlow();
        if (flow != null && (flow >= max)) {
          max = flow;
          maxInfo = item;
        }
      }
      return maxInfo;
    }
  }

  /**
   * Format the results of the Max Lane flow calculation to a TableRow, to save to BigQuery. Add the
   * timestamp from the window context.
   */
  static class FormatMaxesFn extends DoFn<KV<String, LaneInfo>, TableRow> {
    private static final long serialVersionUID = 0;

    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext c) {

      LaneInfo laneInfo = c.element().getValue();
      TableRow row =
          new TableRow()
              .set("station_id", c.element().getKey())
              .set("direction", laneInfo.getDirection())
              .set("freeway", laneInfo.getFreeway())
              .set("lane_max_flow", laneInfo.getLaneFlow())
              .set("lane", laneInfo.getLane())
              .set("avg_occ", laneInfo.getLaneAO())
              .set("avg_speed", laneInfo.getLaneAS())
              .set("total_flow", laneInfo.getTotalFlow())
              .set("recorded_timestamp", laneInfo.getRecordedTimestamp())
              .set("window_timestamp", c.timestamp().toString());
      c.output(row);
    }

    /** Defines the BigQuery schema used for the output. */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("station_id").setType("STRING"));
      fields.add(new TableFieldSchema().setName("direction").setType("STRING"));
      fields.add(new TableFieldSchema().setName("freeway").setType("STRING"));
      fields.add(new TableFieldSchema().setName("lane_max_flow").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("lane").setType("STRING"));
      fields.add(new TableFieldSchema().setName("avg_occ").setType("FLOAT"));
      fields.add(new TableFieldSchema().setName("avg_speed").setType("FLOAT"));
      fields.add(new TableFieldSchema().setName("total_flow").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("recorded_timestamp").setType("STRING"));
      return new TableSchema().setFields(fields);
    }
  }

  /**
   * This PTransform extracts lane info, calculates the max lane flow found for a given station (for
   * the current Window) using a custom 'combiner', and formats the results for BigQuery.
   */
  static class MaxLaneFlow
      extends PTransform<PCollection<KV<String, LaneInfo>>, PCollection<TableRow>> {
    private static final long serialVersionUID = 0;

    @Override
    public PCollection<TableRow> expand(PCollection<KV<String, LaneInfo>> flowInfo) {
      // stationId, LaneInfo => stationId + max lane flow info
      PCollection<KV<String, LaneInfo>> flowMaxes =
          flowInfo.apply(Combine.<String, LaneInfo>perKey(new MaxFlow()));

      // <stationId, max lane flow info>... => row...
      return flowMaxes.apply(ParDo.of(new FormatMaxesFn()));
    }
  }

  /**
   * Options supported by {@link TrafficMaxLaneFlow}.
   *
   * <p>Inherits standard configuration options.
   */
  private interface TrafficMaxLaneFlowOptions
      extends DataflowExampleOptions, ExamplePubsubTopicOptions, ExampleBigQueryTableOptions {
    @Description("Input file to inject to Pub/Sub topic")
    @Default.String(
        "gs://dataflow-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15.csv")
    String getInputFile();

    @SuppressWarnings("unused")
    void setInputFile(String value);

    @Description("Numeric value of sliding window duration, in minutes")
    @Default.Integer(WINDOW_DURATION)
    Integer getWindowDuration();

    @SuppressWarnings("unused")
    void setWindowDuration(Integer value);

    @Description("Numeric value of window 'slide every' setting, in minutes")
    @Default.Integer(WINDOW_SLIDE_EVERY)
    Integer getWindowSlideEvery();

    @SuppressWarnings("unused")
    void setWindowSlideEvery(Integer value);
  }

  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    TrafficMaxLaneFlowOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TrafficMaxLaneFlowOptions.class);
    if (options.isStreaming()) {
      // In order to cancel the pipelines automatically,
      // {@literal DataflowPipelineRunner} is forced to be used.
      options.setRunner(DataflowRunner.class);
    }
    options.setBigQuerySchema(FormatMaxesFn.getSchema());
    // Using DataflowExampleUtils to set up required resources.
    DataflowExampleUtils dataflowUtils = new DataflowExampleUtils(options);
    dataflowUtils.setup();

    Pipeline pipeline = Pipeline.create(options);
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(options.getProject());
    tableRef.setDatasetId(options.getBigQueryDataset());
    tableRef.setTableId(options.getBigQueryTable());

    PCollection<String> rawInput;
    PCollection<KV<String, LaneInfo>> input;

    if (options.isStreaming()) {
      rawInput =
          pipeline.apply(
              "StreamFromPubsub",
              PubsubIO.<String>read()
                  .withCoder(StringUtf8Coder.of())
                  .topic(options.getPubsubTopic()));
    } else {
      rawInput = pipeline.apply("ReadFromFile", TextIO.Read.from(options.getInputFile()));
    }

    // row... => <stationId, LaneInfo> ...
    // if isStreaming == true, do not emit timestamps. If false, emit timestamps.
    input =
        rawInput.apply("ExtractFlowInfo", ParDo.of(new ExtractFlowInfoFn(!options.isStreaming())));

    // map the incoming data stream into sliding windows.
    input
        .apply(
            Window.<KV<String, LaneInfo>>into(
                SlidingWindows.of(Duration.standardMinutes(options.getWindowDuration()))
                    .every(Duration.standardMinutes(options.getWindowSlideEvery()))))
        .apply(new MaxLaneFlow())
        .apply(BigQueryIO.Write.to(tableRef).withSchema(FormatMaxesFn.getSchema()));

    PipelineResult result = pipeline.run();
    /*
    if (options.isStreaming() && !options.getInputFile().isEmpty()) {
      // Inject the data into the Pub/Sub topic with a Dataflow batch pipeline.
      dataflowUtils.runInjectorPipeline(options.getInputFile(), options.getPubsubTopic());
    }
    */

    // dataflowUtils will try to cancel the pipeline and the injector before the program exists.
    dataflowUtils.waitToFinish(result);
  }

  private static Integer tryIntParse(String number) {
    try {
      return Integer.parseInt(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Double tryDoubleParse(String number) {
    try {
      return Double.parseDouble(number);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}

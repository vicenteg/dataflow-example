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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A batch Dataflow pipeline for injecting a set of GCS files into a PubSub topic line by line.
 * Empty lines are skipped.
 *
 * <p>This is useful for testing streaming pipelines. Note that since batch pipelines might retry
 * chunks, this does _not_ guarantee exactly-once injection of file data. Some lines may be
 * published multiple times.
 */
public class PubsubFileInjector {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubFileInjector.class);

  /** A DoFn that publishes non-empty lines to Google Cloud PubSub. */
  public static class FilterHeaderAndEmpties extends DoFn<String, String> {
    @ProcessElement
    @SuppressWarnings("unused")
    public void processElement(ProcessContext c) throws IOException {
      if (!c.element().isEmpty() && !c.element().startsWith("Timestamp")) {
        c.output(c.element());
      } else {
        LOG.info("Not emitting because it's empty or appears to be a header line: " + c.element());
      }
    }
  }

  /** Command line parameter options. */
  private interface PubsubFileInjectorOptions extends PipelineOptions {
    @Description("GCS location of files.")
    @Validation.Required
    String getInput();

    @SuppressWarnings("unused")
    void setInput(String value);

    @Description("Topic to publish on.")
    @Validation.Required
    String getOutputTopic();

    @SuppressWarnings("unused")
    void setOutputTopic(String value);
  }

  /** Sets up and starts streaming pipeline. */
  public static void main(String[] args) {
    PubsubFileInjectorOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubsubFileInjectorOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(TextIO.Read.from(options.getInput()))
        .apply(ParDo.of(new FilterHeaderAndEmpties()))
        .apply(
            PubsubIO.<String>write()
                .topic(options.getOutputTopic())
                .withCoder(StringUtf8Coder.of())
                .timestampLabel("timestamp"));

    pipeline.run();
  }
}

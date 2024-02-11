package com.dataflow.pipeline;

import com.dataflow.transforms.Transformer1;
import com.dataflow.transforms.Transformer2;
import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestDataflowPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(TestDataflowPipeline.class);

    public static void main(String[] args) {

        DataflowPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);

        DataflowProfilingOptions.DataflowProfilingAgentConfiguration agent = new DataflowProfilingOptions.DataflowProfilingAgentConfiguration();
        agent.put("APICurated", true);

        options.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);
        options.setWorkerDiskType("compute.googleapis.com/projects//zones//diskTypes/pd-ssd");
        options.setExperiments(ImmutableList.of("enable_stackdriver_agent_metrics"));
        options.setProfilingAgentConfiguration(agent);

        PipelineResult pr = run(options);
    }

    public static PipelineResult run(DataflowPipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        /*final List<String> messages =
                Arrays.asList("{\n" +
                                "  \"field1\": \"test\",\n" +
                                "  \"field2\": \"test\"" +
                                "}");

          PCollection<String> input = pipeline.apply(Create.of(messages)).setCoder(StringUtf8Coder.of());*/


        // read the messages from pub-sub
        PCollection<String> input =
                pipeline
                        .apply("ReadPubSub",
                                PubsubIO.readStrings()
                                        .fromSubscription("google-input-pub-sub-topic"))
                        .apply(Window.<String>into(FixedWindows.of(Duration.millis(1)))
                                        .triggering(AfterPane.elementCountAtLeast(1))
                                        .withAllowedLateness(Duration.ZERO)
                                        .discardingFiredPanes());


        LOG.info("Invoking Transformer 1");
        PCollectionTuple tuple =
                input.apply(
                        ParDo.of(new Transformer1())
                                .withOutputTags(
                                        Transformer1.successTag,
                                        TupleTagList.of(Transformer1.failureTag)));

        PCollection<String> output = tuple.get(Transformer1.successTag);
        PCollection<String> errors = tuple.get(Transformer1.failureTag);


        LOG.info("Invoking Transformer 2");
        tuple =
                output.apply(
                        ParDo.of(new Transformer2())
                                .withOutputTags(
                                        Transformer2.successTag,
                                        TupleTagList.of(Transformer2.failureTag)));

        LOG.info("Get output pcollection using successtag after Transformer1 transform");
        output = tuple.get(Transformer2.successTag);

        LOG.info("pushing messages to pub-sub");

        // write the messages from pub-sub
        /*output.apply(
                ParDo.of(
                        new DoFn<String, String>() {
                            @ProcessElement
                            public void processElement(@Element String input, OutputReceiver<String> out) {
                                System.out.println(input);
                            }}));*/

        output.apply("WriteToPubSub",
                PubsubIO.writeStrings().to("google-output-pub-sub-topic"));

        return pipeline.run();
    }
}

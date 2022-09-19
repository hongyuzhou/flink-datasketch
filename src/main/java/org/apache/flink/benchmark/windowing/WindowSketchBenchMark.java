package org.apache.flink.benchmark.windowing;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.data.Tuple3SourceGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.datastream.SketchWindowedStream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowSketchBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(WindowSketchBenchMark.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
                //.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        long rowsPerSecond = Long.parseLong(params.get("rowsPerSecond","25000"));
        long numberOfRows = Long.parseLong(params.get("numberOfRows","1000000000"));

        DataStream<Tuple3<String, Long, String>> source = env
                .addSource(new DataGeneratorSource<>(
                        new Tuple3SourceGenerator(),
                        rowsPerSecond,
                        numberOfRows))
                .returns(new TypeHint<Tuple3<String, Long, String>>() {})
                .name("source");

        WindowedStream<Tuple3<String, Long, String>, String, TimeWindow> window =
                source
                        .keyBy(value -> value.f0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(30)));
        SketchWindowedStream<Tuple3<String, Long, String>, String, TimeWindow> sketchWindowedStream =
                new SketchWindowedStream<>(window, env.getConfig());


        DataStream<Double> estimate;
        if ("cpc".equals(params.get("sketch", "hll"))) {
            estimate = sketchWindowedStream
                    .cpc(2).name("cpc");
                    //.aggregate(new CpcAggregate()).name("cpc");
        } else {
            estimate = sketchWindowedStream
                    .hll(2).name("hll");
                    //.aggregate(new HllAggregate()).name("hll");
        }
        estimate.print();
        env.execute();
    }
}

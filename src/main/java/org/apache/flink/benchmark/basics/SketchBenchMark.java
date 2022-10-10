package org.apache.flink.benchmark.basics;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.data.Tuple3SourceGenerator;
import org.apache.flink.benchmark.operator.process.CpcKeyedProcess;
import org.apache.flink.benchmark.operator.process.HllKeyedProcess;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.datastream.SketchKeyedStream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SketchBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(SketchBenchMark.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
                //.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        long rowsPerSecond = Long.parseLong(params.get("rowsPerSecond", "25000"));
        long numberOfRows = Long.parseLong(params.get("numberOfRows", "1000000000"));

        DataStream<Tuple3<String, Long, String>> source = env
                .addSource(new DataGeneratorSource<>(
                        new Tuple3SourceGenerator(),
                        rowsPerSecond,
                        numberOfRows))
                .returns(new TypeHint<Tuple3<String, Long, String>>() {})
                .name("source");

        KeyedStream<Tuple3<String, Long, String>, String> keyed = source
                .keyBy(value -> value.f0);

        SketchKeyedStream<Tuple3<String, Long, String>, String> sketchKeyedStream = new SketchKeyedStream<>(keyed);

        DataStream<Double> estimate;
        if ("cpc".equals(params.get("sketch", "hll"))) {
            estimate = sketchKeyedStream
                    .cpc(2).name("cpc");
                    //.process(new CpcKeyedProcess()).name("cpc");
        } else {
            estimate = sketchKeyedStream
                    .hll(2).name("hll");
                    //.process(new HllKeyedProcess()).name("hll");
        }
        estimate.print();
        env.execute();
    }
}

package org.apache.flink.benchmark.windowing;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.data.Tuple3SourceGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.benchmark.operator.aggregate.CpcAggregate;
import org.apache.flink.benchmark.operator.aggregate.HllAggregate;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AllWindowSketchBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(AllWindowSketchBenchMark.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        DataStream<Tuple3<String, Long, String>> source = env
                .addSource(new DataGeneratorSource<>(
                        new Tuple3SourceGenerator(),
                        3500000L,
                        1000000000L))
                // 产出10亿行，每秒350万行
                .returns(new TypeHint<Tuple3<String, Long, String>>() {})
                .name("source");

        AllWindowedStream<Tuple3<String, Long, String>, TimeWindow> windowAll = source
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));

        DataStream<Double> estimate;

        if ("cpc".equals(params.get("sketch", "hll"))) {
            estimate = windowAll
                    //.cpc(2).name("cpc");
                    .aggregate(new CpcAggregate()).name("cpc");
        } else {
            estimate = windowAll
                    //.hll(2).name("hll");
                    .aggregate(new HllAggregate()).name("hll");
        }
        estimate.print();
        env.execute();
    }
}

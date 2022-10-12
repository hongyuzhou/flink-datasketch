package org.apache.flink.benchmark.basics;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.data.Tuple3SourceGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.serializer.SetSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class WithoutSketchBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(WithoutSketchBenchMark.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        //.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(4);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        long rowsPerSecond = Long.parseLong(params.get("rowsPerSecond", "25000"));
        long numberOfRows = Long.parseLong(params.get("numberOfRows", "1000000000"));

        DataStream<Tuple3<String, Long, String>> source = env
                .addSource(new DataGeneratorSource<>(
                        new Tuple3SourceGenerator(),
                        rowsPerSecond,
                        numberOfRows))
                .returns(new TypeHint<Tuple3<String, Long, String>>() {})
                .name("source");

        DataStream<Record<Tuple3<String, Long, String>>> estimate = source
                .keyBy(value -> value.f0)
                .process(new CountDistinctProcessFunction());

        estimate.print();
        env.execute("WithoutSketchBenchMark");
    }

    private static class CountDistinctProcessFunction extends KeyedProcessFunction<String, Tuple3<String, Long, String>, Record<Tuple3<String, Long, String>>> {

        private ValueState<Set<String>> distinct;

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Set<String>> distinctStateDescriptor
                    = new ValueStateDescriptor<>("DistinctSetState", new SetSerializer<>(new StringSerializer()));
            distinct = getRuntimeContext().getState(distinctStateDescriptor);
        }

        @Override
        public void processElement(Tuple3<String, Long, String> value, Context ctx, Collector<Record<Tuple3<String, Long, String>>> out) throws Exception {

            Set<String> set = distinct.value();
            if (set == null) {
                set = new HashSet<>();
            }
            set.add(value.f2);

            distinct.update(set);

            out.collect(new Record<>(value, set.size()));
        }
    }

    private static class Record<T> {
        private T val;
        private Integer uv = 0;

        public Record(T val, Integer uv) {
            this.val = val;
            this.uv = uv;
        }

        public T getVal() {
            return val;
        }

        public Integer getUv() {
            return uv;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "val=" + val +
                    ", uv=" + uv +
                    '}';
        }
    }
}

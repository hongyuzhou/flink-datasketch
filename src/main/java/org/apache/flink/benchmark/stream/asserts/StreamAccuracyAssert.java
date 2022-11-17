package org.apache.flink.benchmark.stream.asserts;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.data.ShakespeareDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.serializer.CpcTypeSerializer;
import org.apache.flink.core.serializer.HllTypeSerializer;
import org.apache.flink.core.serializer.SetSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StreamAccuracyAssert {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        long rowsPerSecond = params.getLong("rowsPerSecond", 200);
        long numberOfRows = params.getLong("numberOfRows", Long.MAX_VALUE);

        boolean useHll = params.getBoolean("useHll", true);
        env.addSource(new DataGeneratorSource<>(new ShakespeareDataGenerator(), rowsPerSecond, numberOfRows))
                .returns(String.class)
                .name("source")
                .flatMap(new Tokenizer())
                .keyBy(t -> t.f0)
                .process(new ShakespeareTokenUv(useHll))
                .writeAsCsv("accuracy-assert2.csv", FileSystem.WriteMode.OVERWRITE)
//                .addSink(StreamingFileSink.
//                        <Tuple3<Long, Integer, Double>>forRowFormat(new Path("accuracy-assert"), new SimpleStringEncoder<>())
//                        .withRollingPolicy(
//                                DefaultRollingPolicy.builder()
//                                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
//                                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
//                                        .withMaxPartSize(1024 * 1024 * 1024)
//                                        .build())
//                        .withOutputFileConfig(
//                                OutputFileConfig
//                                        .builder()
//                                        .withPartPrefix("error")
//                                        .withPartSuffix("csv")
//                                        .build())
//                        .build())
        ;

        env.execute("StreamAccuracyAssert");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple word in the
     * form of "(shakespeare,word)" ({@code Tuple2<String, String>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, String>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, String>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(Tuple2.of("shakespeare", token));
                }
            }
        }
    }

    public static final class ShakespeareTokenUv extends KeyedProcessFunction<String, Tuple2<String, String>, Tuple3<Long, Integer, Double>> {

        private ValueState<Set<String>> distinct;

        private ValueState<HllSketch> hll;

        private ValueState<CpcSketch> cpc;

        private ValueState<Long> count;

        private boolean useHll;

        public ShakespeareTokenUv(boolean useHll) {
            this.useHll = useHll;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Set<String>> distinctStateDescriptor
                    = new ValueStateDescriptor<>("DistinctSetState", new SetSerializer<>(new StringSerializer()));
            distinct = getRuntimeContext().getState(distinctStateDescriptor);

            ValueStateDescriptor<HllSketch> hllSketchStateDescriptor = new ValueStateDescriptor<>("HllSketchState", new HllTypeSerializer());
            hll = getRuntimeContext().getState(hllSketchStateDescriptor);

            ValueStateDescriptor<CpcSketch> cpcSketchStateDescriptor = new ValueStateDescriptor<>("CpcSketchState",
                    new CpcTypeSerializer(CpcSketch.class, getRuntimeContext().getExecutionConfig()));
            cpc = getRuntimeContext().getState(cpcSketchStateDescriptor);

            ValueStateDescriptor<Long> countState
                    = new ValueStateDescriptor<>("RecordCountState", Long.class);
            count = getRuntimeContext().getState(countState);
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple3<Long, Integer, Double>> out) throws Exception {
            Long cnt = count.value();
            if (cnt == null) {
                cnt = 0L;
            }
            cnt += 1;
            count.update(cnt);


            Set<String> set = distinct.value();
            if (set == null) {
                set = new HashSet<>();
            }
            set.add(value.f1);
            distinct.update(set);


            HllSketch hllSketch = hll.value();
            if (hllSketch == null) {
                hllSketch = new HllSketch();
            }
            hllSketch.update(value.f1);
            hll.update(hllSketch);

            CpcSketch cpcSketch = cpc.value();
            if (cpcSketch == null) {
                cpcSketch = new CpcSketch();
            }
            cpcSketch.update(value.f1);
            cpc.update(cpcSketch);

            if (cnt % 5000 == 0) {
                out.collect(Tuple3.of(cnt, set.size(), useHll ? hllSketch.getEstimate() : cpcSketch.getEstimate()));
            }
        }
    }
}

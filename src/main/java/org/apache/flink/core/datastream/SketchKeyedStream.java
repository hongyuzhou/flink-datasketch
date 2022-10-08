package org.apache.flink.core.datastream;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.function.process.distinct.impl.CpcAccumulator;
import org.apache.flink.core.function.process.distinct.impl.HllAccumulator;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

@Public
public class SketchKeyedStream<T, KEY> {

    private final KeyedStream<T, KEY> inner;

    private final ExecutionConfig config;

    public SketchKeyedStream(KeyedStream<T, KEY> inner, ExecutionConfig config) {
        this.inner = inner;
        this.config = config;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> hll(int positionToSum) {
        return inner.process(new HllAccumulator<>(
                positionToSum, inner.getType(), config));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> hll(int positionToSum, int lgConfigK, TgtHllType tgtHllType, boolean selfTypeSerializer) {
        return inner.process(new HllAccumulator<>(
                positionToSum, inner.getType(), config, lgConfigK, tgtHllType, selfTypeSerializer));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> cpc(int positionToSum) {
        return inner.process(new CpcAccumulator<>(
                positionToSum, inner.getType(), config));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> cpc(int positionToSum, int lgk, long seed) {
        return inner.process(new CpcAccumulator<>(
                positionToSum, inner.getType(), config, lgk, seed));
    }

    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> process(KeyedProcessFunction<KEY, T, R> function) {
        return inner.process(function);
    }

}

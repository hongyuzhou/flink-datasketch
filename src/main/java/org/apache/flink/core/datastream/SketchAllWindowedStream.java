package org.apache.flink.core.datastream;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.core.function.aggregate.distinct.impl.CpcAccumulator;
import org.apache.flink.core.function.aggregate.distinct.impl.HllAccumulator;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

@Public
public class SketchAllWindowedStream<T, W extends Window> {

    private final AllWindowedStream<T, W> inner;

    private final ExecutionConfig config;

    public SketchAllWindowedStream(AllWindowedStream<T, W> inner, ExecutionConfig config) {
        this.inner = inner;
        this.config = config;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> hll(int positionToSum) {
        return inner.aggregate(new HllAccumulator<>(
                positionToSum, inner.getInputType(), config));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> hll(int positionToSum, int lgConfigK, TgtHllType tgtHllType) {
        return inner.aggregate(new HllAccumulator<>(
                positionToSum, inner.getInputType(), config, lgConfigK, tgtHllType));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> cpc(int positionToSum) {
        return inner.aggregate(new CpcAccumulator<>(
                positionToSum, inner.getInputType(), config));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<Double> cpc(int positionToSum, int lgk, long seed) {
        return inner.aggregate(new CpcAccumulator<>(
                positionToSum, inner.getInputType(), config, lgk, seed));
    }

    @PublicEvolving
    public <ACC, R> SingleOutputStreamOperator<R> aggregate(AggregateFunction<T, ACC, R> function) {
        return inner.aggregate(function);
    }

    @PublicEvolving
    public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, V> aggFunction,
            ProcessAllWindowFunction<V, R, W> windowFunction) {
        return inner.aggregate(aggFunction, windowFunction);
    }

    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> process(ProcessAllWindowFunction<T, R, W> function) {
        return inner.process(function);
    }

}

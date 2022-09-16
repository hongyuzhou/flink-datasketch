package org.apache.flink.core.datastream;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.core.function.aggregate.impl.CpcAccumulator;
import org.apache.flink.core.function.aggregate.impl.HllAccumulator;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

@Public
public class SketchWindowedStream<T, K, W extends Window> {

    private final WindowedStream<T, K, W> inner;

    private final ExecutionConfig config;

    public SketchWindowedStream(WindowedStream<T, K, W> inner, ExecutionConfig config) {
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

}

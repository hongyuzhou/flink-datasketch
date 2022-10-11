package org.apache.flink.core.datastream;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.function.process.distinct.impl.CpcAccumulator;
import org.apache.flink.core.function.process.distinct.impl.HllAccumulator;
import org.apache.flink.core.function.process.distinct.impl.SketchRecord;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

@Public
public class SketchKeyedStream<T, KEY> {

    private final KeyedStream<T, KEY> inner;

    public SketchKeyedStream(KeyedStream<T, KEY> inner) {
        this.inner = inner;
    }

    @PublicEvolving
    public SingleOutputStreamOperator<SketchRecord<T>> hll(int positionToSum) {
        return inner.process(new HllAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig()));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<SketchRecord<T>> hll(int positionToSum, int lgConfigK, TgtHllType tgtHllType, boolean selfTypeSerializer) {
        return inner.process(new HllAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig(), lgConfigK, tgtHllType, selfTypeSerializer));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<SketchRecord<T>> cpc(int positionToSum) {
        return inner.process(new CpcAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig()));
    }

    @PublicEvolving
    public SingleOutputStreamOperator<SketchRecord<T>> cpc(int positionToSum, int lgk, long seed) {
        return inner.process(new CpcAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig(), lgk, seed));
    }

    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> process(KeyedProcessFunction<KEY, T, R> function) {
        return inner.process(function);
    }

}

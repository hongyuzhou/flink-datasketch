package org.apache.flink.core.datastream;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.core.function.aggregate.distinct.impl.CpcAccumulator;
import org.apache.flink.core.function.aggregate.distinct.impl.HllAccumulator;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * SketchWindowedStream
 *
 * @param <T> The type of elements in the stream.
 * @param <K> The type of the key by which elements are grouped.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns the elements to.
 */
@Public
public class SketchWindowedStream<T, K, W extends Window> {

    private final WindowedStream<T, K, W> inner;

    private final ExecutionConfig config;

    public SketchWindowedStream(WindowedStream<T, K, W> inner, ExecutionConfig config) {
        this.inner = inner;
        this.config = config;
    }

    /**
     * @param positionToSum The field position in the data points to use HllSketch {@link HllSketch}. This is applicable to
     *                      Tuple types, basic and primitive array types, and primitive types
     *                      (which is considered as having one field).
     * @return The transformed DataStream.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<Double> hll(int positionToSum) {
        return inner.aggregate(new HllAccumulator<>(
                positionToSum, inner.getInputType(), config));
    }

    /**
     * @param positionToSum The field position in the data points to use HllSketch {@link HllSketch}. This is applicable to
     *                      Tuple types, basic and primitive array types, and primitive types
     *                      (which is considered as having one field).
     * @param lgConfigK     The Log2 of K for the target HLL sketch. This value must be
     *                      between 4 and 21 inclusively.
     * @param tgtHllType    the desired Hll type.
     * @return The transformed DataStream.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<Double> hll(int positionToSum, int lgConfigK, TgtHllType tgtHllType) {
        return inner.aggregate(new HllAccumulator<>(
                positionToSum, inner.getInputType(), config, lgConfigK, tgtHllType));
    }

    /**
     * @param positionToSum The field position in the data points to use CpcSketch {@link CpcSketch}. This is applicable to
     *                      Tuple types, basic and primitive array types, and primitive types
     *                      (which is considered as having one field).
     * @return The transformed DataStream.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<Double> cpc(int positionToSum) {
        return inner.aggregate(new CpcAccumulator<>(
                positionToSum, inner.getInputType(), config));
    }

    /**
     * @param positionToSum The field position in the data points to use CpcSketch {@link CpcSketch}. This is applicable to
     *                      Tuple types, basic and primitive array types, and primitive types
     *                      (which is considered as having one field).
     * @param lgk           the given log_base2 of k
     * @param seed          the given seed
     * @return The transformed DataStream.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<Double> cpc(int positionToSum, int lgk, long seed) {
        return inner.aggregate(new CpcAccumulator<>(
                positionToSum, inner.getInputType(), config, lgk, seed));
    }

    /**
     * {@link WindowedStream#aggregate(AggregateFunction)}
     *
     * @param function The aggregation function.
     * @param <ACC>    The type of the AggregateFunction's accumulator
     * @param <R>      The type of the elements in the resulting stream, equal to the AggregateFunction's
     *                 result type
     * @return The data stream that is the result of applying the fold function to the window.
     * @see WindowedStream#aggregate(AggregateFunction)
     */
    @PublicEvolving
    public <ACC, R> SingleOutputStreamOperator<R> aggregate(AggregateFunction<T, ACC, R> function) {
        return inner.aggregate(function);
    }

    /**
     * {@link WindowedStream#aggregate(AggregateFunction, ProcessWindowFunction)}
     *
     * @param aggFunction    The aggregate function that is used for incremental aggregation.
     * @param windowFunction The window function.
     * @param <ACC>          The type of the AggregateFunction's accumulator
     * @param <V>            The type of AggregateFunction's result, and the WindowFunction's input
     * @param <R>            The type of the elements in the resulting stream, equal to the WindowFunction's
     *                       result type
     * @return The data stream that is the result of applying the window function to the window.
     * @see WindowedStream#aggregate(AggregateFunction, ProcessWindowFunction)
     */
    @PublicEvolving
    public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, V> aggFunction,
            ProcessWindowFunction<V, R, K, W> windowFunction) {
        return inner.aggregate(aggFunction, windowFunction);
    }

    /**
     * {@link WindowedStream#process(ProcessWindowFunction)}
     *
     * @param function The window function.
     * @return The data stream that is the result of applying the window function to the window.
     * @see WindowedStream#process(ProcessWindowFunction)
     */
    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> process(ProcessWindowFunction<T, R, K, W> function) {
        return inner.process(function);
    }


}

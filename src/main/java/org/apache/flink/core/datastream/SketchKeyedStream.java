package org.apache.flink.core.datastream;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.function.process.distinct.impl.CpcAccumulator;
import org.apache.flink.core.function.process.distinct.impl.HllAccumulator;
import org.apache.flink.core.function.process.distinct.impl.SketchRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * SketchKeyedStream
 *
 * @param <T>   The type of the elements in the SketchKeyed Stream.
 * @param <KEY> The type of the key in the SketchKeyed Stream.
 * @see KeyedStream
 */
@Public
public class SketchKeyedStream<T, KEY> {

    private final KeyedStream<T, KEY> inner;

    public SketchKeyedStream(KeyedStream<T, KEY> inner) {
        this.inner = inner;
    }

    /**
     * @param positionToSum The field position in the data points to use HllSketch {@link HllSketch}. This is applicable to
     *                      Tuple types, basic and primitive array types, and primitive types
     *                      (which is considered as having one field).
     * @return The transformed DataStream.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<SketchRecord<T>> hll(int positionToSum) {
        return inner.process(new HllAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig()));
    }

    /**
     * @param positionToSum      The field position in the data points to use HllSketch {@link HllSketch}. This is applicable to
     *                           Tuple types, basic and primitive array types, and primitive types
     *                           (which is considered as having one field).
     * @param lgConfigK          The Log2 of K for the target HLL sketch. This value must be
     *                           between 4 and 21 inclusively.
     * @param tgtHllType         the desired Hll type.
     * @param selfTypeSerializer Use HllTypeSerializer or KryoSerializer
     * @return The transformed DataStream.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<SketchRecord<T>> hll(int positionToSum, int lgConfigK, TgtHllType tgtHllType, boolean selfTypeSerializer) {
        return inner.process(new HllAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig(), lgConfigK, tgtHllType, selfTypeSerializer));
    }

    /**
     * @param positionToSum The field position in the data points to use CpcSketch {@link CpcSketch}. This is applicable to
     *                      Tuple types, basic and primitive array types, and primitive types
     *                      (which is considered as having one field).
     * @return The transformed DataStream.
     */
    @PublicEvolving
    public SingleOutputStreamOperator<SketchRecord<T>> cpc(int positionToSum) {
        return inner.process(new CpcAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig()));
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
    public SingleOutputStreamOperator<SketchRecord<T>> cpc(int positionToSum, int lgk, long seed) {
        return inner.process(new CpcAccumulator<>(
                positionToSum, inner.getType(), inner.getExecutionConfig(), lgk, seed));
    }

    /**
     * {@link KeyedStream#process(KeyedProcessFunction)}
     *
     * @param function The {@link KeyedProcessFunction} that is called for each element
     *                 in the stream.
     * @param <R>      The type of elements emitted by the {@code KeyedProcessFunction}.
     * @return The transformed {@link DataStream}.
     * @see KeyedStream#process(KeyedProcessFunction)
     */
    @PublicEvolving
    public <R> SingleOutputStreamOperator<R> process(KeyedProcessFunction<KEY, T, R> function) {
        return inner.process(function);
    }

}

package org.apache.flink.core.function.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * SketchAggregateFunction
 *
 * @param <IN>  The type of the values that are aggregated (input values)
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> The type of the aggregated result
 * @see AggregateFunction
 */
public abstract class SketchAggregateFunction<IN, ACC, OUT> implements AggregateFunction<IN, ACC, OUT> {

    private static final long serialVersionUID = 1L;

}

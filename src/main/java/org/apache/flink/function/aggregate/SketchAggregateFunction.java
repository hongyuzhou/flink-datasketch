package org.apache.flink.function.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;

public abstract class SketchAggregateFunction<IN, ACC, OUT> implements AggregateFunction<IN, ACC, OUT> {

    private static final long serialVersionUID = 1L;
}

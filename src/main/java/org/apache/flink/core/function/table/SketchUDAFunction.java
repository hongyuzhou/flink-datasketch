package org.apache.flink.core.function.table;

import org.apache.flink.table.functions.AggregateFunction;

public abstract class SketchUDAFunction<T, ACC> extends AggregateFunction<T, ACC> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isDeterministic() {
        return false;
    }
}

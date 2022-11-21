package org.apache.flink.core.function.table;

import org.apache.flink.table.functions.TableAggregateFunction;

/**
 * SketchUDAFunction
 *
 * @param <T>   final result type of the aggregation
 * @param <ACC> intermediate result type during the aggregation
 * @see TableAggregateFunction
 */
public abstract class SketchUDTAFunction<T, ACC> extends TableAggregateFunction<T, ACC> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isDeterministic() {
        return false;
    }
}

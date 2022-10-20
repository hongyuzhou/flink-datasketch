package org.apache.flink.core.function.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

/**
 * SketchKeyedProcessFunction
 *
 * @param <K> Type of the key.
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 * @see KeyedProcessFunction
 */
public abstract class SketchKeyedProcessFunction<K, I, O> extends KeyedProcessFunction<K, I, O> {

    private static final long serialVersionUID = 1L;

    private Counter counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("PV Count");
    }

    /**
     * PV count
     */
    public void pvCountInc() {
        if (counter != null) {
            counter.inc();
        }
    }
}

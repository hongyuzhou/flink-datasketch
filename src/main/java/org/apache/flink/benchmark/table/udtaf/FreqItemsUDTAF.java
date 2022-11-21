package org.apache.flink.benchmark.table.udtaf;

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.core.function.table.frequencies.ItemsUDTAFunction;
import org.apache.flink.util.Collector;


/**
 * Implementation of ItemsUDTAFunction
 */
public class FreqItemsUDTAF extends ItemsUDTAFunction<Long, Long> {

    private static final long serialVersionUID = 1L;

    public FreqItemsUDTAF(int maxMapSize, int topK) {
        super(maxMapSize, topK);
    }

    @Override
    public ItemsSketch<Long> createAccumulator() {
        return super.createAccumulator();
    }

    public void accumulate(ItemsSketch<Long> acc, Long iValue) {
        if (iValue != null) {
            acc.update(iValue);
        }
    }


    public void emitValue(ItemsSketch<Long> accumulator, Collector<Long> out) {
        final ItemsSketch.Row<Long>[] result = accumulator.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        for (int i = 0; i < Math.min(result.length, topK); i++) {
            out.collect(result[i].getItem());
        }
    }

    public void resetAccumulator(ItemsSketch<Long> acc) {
        super.resetAccumulator(acc);
    }

    @Override
    public void merge(ItemsSketch<Long> acc, Iterable<ItemsSketch<Long>> it) {
        super.merge(acc, it);
    }
}
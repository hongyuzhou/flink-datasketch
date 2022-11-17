package org.apache.flink.core.function.table.frequencies;

import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.core.function.table.SketchUDAFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;

/**
 * Use ItemsSketch {@link ItemsSketch} As Accumulator For ItemsUDAFunction
 *
 * @param <T>  final result type of the aggregation
 * @param <IT> data type of the ItemsSketch
 */
@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = ItemsSketch.class)
)
public abstract class ItemsUDAFunction<T, IT> extends SketchUDAFunction<T, ItemsSketch<IT>> {

    private static final long serialVersionUID = 1L;

    protected int maxMapSize;

    protected int topK;

    public ItemsUDAFunction(int maxMapSize, int topK) {
        this.maxMapSize = maxMapSize;
        this.topK = topK;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    public ItemsSketch<IT> createAccumulator() {
        return new ItemsSketch<>(maxMapSize);
    }

    public void resetAccumulator(ItemsSketch<IT> acc) {
        acc.reset();
    }


    public void merge(ItemsSketch<IT> acc, Iterable<ItemsSketch<IT>> it) {
        for (ItemsSketch<IT> itemsSketch : it) {
            acc.merge(itemsSketch);
        }
    }
}
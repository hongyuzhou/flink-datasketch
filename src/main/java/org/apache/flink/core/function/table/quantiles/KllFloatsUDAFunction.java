package org.apache.flink.core.function.table.quantiles;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.function.table.SketchUDAFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;

/**
 * Use KllFloatsSketch {@link KllFloatsSketch} As Accumulator For KllFloatsUDAFunction
 *
 * @param <T> final result type of the aggregation
 */
@Experimental
@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = KllFloatsSketch.class)
)
public abstract class KllFloatsUDAFunction<T> extends SketchUDAFunction<T, KllFloatsSketch> {

    private static final long serialVersionUID = 1L;

    protected int k;

    public KllFloatsUDAFunction() {
        this.k = KllFloatsSketch.DEFAULT_K;
    }

    public KllFloatsUDAFunction(int k) {
        this.k = k;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    public KllFloatsSketch createAccumulator() {
        return KllFloatsSketch.newHeapInstance(k);
    }

    public void resetAccumulator(KllFloatsSketch acc) {
        acc.reset();
    }


    public void merge(KllFloatsSketch acc, Iterable<KllFloatsSketch> it) {
        for (KllFloatsSketch kllFloatsSketch : it) {
            acc.merge(kllFloatsSketch);
        }
    }
}
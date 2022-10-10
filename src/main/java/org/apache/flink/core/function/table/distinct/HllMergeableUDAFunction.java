package org.apache.flink.core.function.table.distinct;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.flink.core.function.table.SketchUDAFunction;
import org.apache.flink.core.serializer.HllTypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;

import static org.apache.datasketches.hll.HllSketch.DEFAULT_LG_K;

@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = Union.class)
)
public abstract class HllMergeableUDAFunction<T> extends SketchUDAFunction<T, Union> {

    private static final long serialVersionUID = 1L;

    protected int lgConfigK;

    protected TgtHllType tgtHllType;

    public HllMergeableUDAFunction() {
        this.lgConfigK = DEFAULT_LG_K;
        this.tgtHllType = TgtHllType.HLL_4;
    }

    public HllMergeableUDAFunction(int lgConfigK, TgtHllType tgtHllType) {
        if ((lgConfigK < 4) || (lgConfigK > 21)) {
            throw new SketchesArgumentException(
                    "Log K must be between 4 and 21, inclusive: " + lgConfigK);
        }
        this.lgConfigK = lgConfigK;
        this.tgtHllType = tgtHllType;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    public Union createAccumulator() {
        return new Union(lgConfigK);
    }

    public void merge(Union acc, Iterable<Union> it) {
        for (Union u : it) {
            acc.update(u.getResult(tgtHllType));
        }
    }

    public void resetAccumulator(Union acc) {
        acc.reset();
    }
}
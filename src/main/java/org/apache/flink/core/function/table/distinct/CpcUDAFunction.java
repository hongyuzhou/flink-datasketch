package org.apache.flink.core.function.table.distinct;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.core.function.table.SketchUDAFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.cpc.CpcSketch.DEFAULT_LG_K;

/**
 * Use CpcSketch {@link CpcSketch} As Accumulator For CpcUDAFunction
 *
 * @param <T> final result type of the aggregation
 */
@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = CpcSketch.class)
)
public abstract class CpcUDAFunction<T> extends SketchUDAFunction<T, CpcSketch> {

    private static final long serialVersionUID = 1L;

    protected int lgK;

    protected long seed;

    public CpcUDAFunction() {
        this.lgK = DEFAULT_LG_K;
        this.seed = DEFAULT_UPDATE_SEED;
    }

    public CpcUDAFunction(int lgK, long seed) {
        if ((lgK < 4) || (lgK > 26)) {
            throw new SketchesArgumentException("LgK must be >= 4 and <= 26: " + lgK);
        }
        this.lgK = lgK;
        this.seed = seed;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    public CpcSketch createAccumulator() {
        return new CpcSketch(lgK, seed);
    }

    public void resetAccumulator(CpcSketch acc) {
        acc.reset();
    }
}
package org.apache.flink.core.function.table.distinct;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.flink.core.function.table.SketchUDAFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.cpc.CpcSketch.DEFAULT_LG_K;

@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = CpcMergeableUDAFunction.CpcTableAcc.class)
)
public abstract class CpcMergeableUDAFunction<T> extends SketchUDAFunction<T, CpcMergeableUDAFunction.CpcTableAcc> {

    private static final long serialVersionUID = 1L;

    protected int lgK;

    protected long seed;

    public CpcMergeableUDAFunction() {
        this.lgK = DEFAULT_LG_K;
        this.seed = DEFAULT_UPDATE_SEED;
    }

    public CpcMergeableUDAFunction(int lgK, long seed) {
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
    public CpcTableAcc createAccumulator() {
        return new CpcTableAcc(new CpcSketch(lgK, seed), new CpcUnion(lgK, seed));
    }

    public void resetAccumulator(CpcTableAcc acc) {
        acc.getCpcSketch().reset();
        acc.getCpcUnion().getResult().reset();
    }

    protected class CpcTableAcc {
        CpcSketch cpcSketch;

        CpcUnion cpcUnion;

        CpcTableAcc(CpcSketch cpcSketch, CpcUnion cpcUnion) {
            this.cpcSketch = cpcSketch;
            this.cpcUnion = cpcUnion;
        }

        public CpcSketch getCpcSketch() {
            return cpcSketch;
        }


        public CpcUnion getCpcUnion() {
            return cpcUnion;
        }
    }
}
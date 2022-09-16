package org.apache.flink.core.function.aggregate.distinct;


import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.core.function.aggregate.SketchAggregateFunction;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.cpc.CpcSketch.DEFAULT_LG_K;

public abstract class CpcAggregateFunction<IN, OUT> extends SketchAggregateFunction<IN, CpcSketch, OUT> {

    private static final long serialVersionUID = 1L;

    protected final int lgK;
    protected final long seed;

    public CpcAggregateFunction() {
        this.lgK = DEFAULT_LG_K;
        this.seed = DEFAULT_UPDATE_SEED;
    }

    public CpcAggregateFunction(int lgK, long seed) {
        if ((lgK < 4) || (lgK > 26)) {
            throw new SketchesArgumentException("LgK must be >= 4 and <= 26: " + lgK);
        }
        this.lgK = lgK;
        this.seed = seed;
    }

    @Override
    public CpcSketch createAccumulator() {
        return new CpcSketch(lgK, seed);
    }
}

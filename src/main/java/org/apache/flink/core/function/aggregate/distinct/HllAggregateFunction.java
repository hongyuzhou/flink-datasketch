package org.apache.flink.core.function.aggregate.distinct;


import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.core.function.aggregate.SketchAggregateFunction;

import static org.apache.datasketches.hll.HllSketch.DEFAULT_LG_K;

/**
 * Use HllSketch {@link HllSketch} For HllAggregateFunction
 *
 * @param <IN>  The type of the values that are aggregated (input values)
 * @param <OUT> The type of the aggregated result
 */
public abstract class HllAggregateFunction<IN, OUT> extends SketchAggregateFunction<IN, HllSketch, OUT> {

    private static final long serialVersionUID = 1L;

    protected final int lgConfigK;
    protected final TgtHllType tgtHllType;

    public HllAggregateFunction() {
        this.lgConfigK = DEFAULT_LG_K;
        this.tgtHllType = TgtHllType.HLL_4;
    }

    public HllAggregateFunction(int lgConfigK, TgtHllType tgtHllType) {
        if ((lgConfigK < 4) || (lgConfigK > 21)) {
            throw new SketchesArgumentException(
                    "Log K must be between 4 and 21, inclusive: " + lgConfigK);
        }
        this.lgConfigK = lgConfigK;
        this.tgtHllType = tgtHllType;
    }

    @Override
    public HllSketch createAccumulator() {
        return new HllSketch(lgConfigK, tgtHllType);
    }
}

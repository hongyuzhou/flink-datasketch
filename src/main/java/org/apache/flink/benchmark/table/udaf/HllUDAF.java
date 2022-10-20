package org.apache.flink.benchmark.table.udaf;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.core.function.table.distinct.HllUDAFunction;

/**
 * Implementation of HllUDAFunction
 */
public class HllUDAF extends HllUDAFunction<Double> {

    private static final long serialVersionUID = 1L;

    public HllUDAF() {
        super();
    }

    public HllUDAF(int lgConfigK, TgtHllType tgtHllType) {
        super(lgConfigK, tgtHllType);
    }

    @Override
    public HllSketch createAccumulator() {
        return super.createAccumulator();
    }

    public void accumulate(HllSketch acc, Long iValue) {
        if (iValue != null) {
            acc.update(iValue);
        }
    }

    public void accumulate(HllSketch acc, String iValue) {
        if(iValue != null) {
            acc.update(iValue);
        }
    }

    public void accumulate(HllSketch acc, Double iValue) {
        if(iValue != null) {
            acc.update(iValue);
        }
    }

    @Override
    public Double getValue(HllSketch accumulator) {
        return accumulator.getEstimate();
    }

    public void resetAccumulator(HllSketch acc) {
        super.resetAccumulator(acc);
    }
}
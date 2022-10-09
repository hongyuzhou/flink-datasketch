package org.apache.flink.benchmark.table.udaf;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.core.function.table.distinct.CpcUDAFunction;


public class CpcUDAF extends CpcUDAFunction<Double> {

    private static final long serialVersionUID = 1L;

    public CpcUDAF() {
        super();
    }

    public CpcUDAF(int lgK, long seed) {
        super(lgK, seed);
    }

    @Override
    public CpcSketch createAccumulator() {
        return super.createAccumulator();
    }

    public void accumulate(CpcSketch acc, Long iValue) {
        if (iValue != null) {
            acc.update(iValue);
        }
    }

    public void accumulate(CpcSketch acc, String iValue) {
        if (iValue != null) {
            acc.update(iValue);
        }
    }

    public void accumulate(CpcSketch acc, Double iValue) {
        if (iValue != null) {
            acc.update(iValue);
        }
    }

    @Override
    public Double getValue(CpcSketch accumulator) {
        return accumulator.getEstimate();
    }

    public void resetAccumulator(CpcSketch acc) {
        super.resetAccumulator(acc);
    }
}
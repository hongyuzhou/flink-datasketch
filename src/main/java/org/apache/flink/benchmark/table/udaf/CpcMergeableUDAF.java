package org.apache.flink.benchmark.table.udaf;

import org.apache.flink.core.function.table.distinct.CpcMergeableUDAFunction;

/**
 * Implementation of CpcMergeableUDAFunction
 */
public class CpcMergeableUDAF extends CpcMergeableUDAFunction<Double> {

    private static final long serialVersionUID = 1L;

    public CpcMergeableUDAF() {
        super();
    }

    public CpcMergeableUDAF(int lgK, long seed) {
        super(lgK, seed);
    }

    @Override
    public CpcTableAcc createAccumulator() {
        return super.createAccumulator();
    }

    public void accumulate(CpcTableAcc acc, Long iValue) {
        if (iValue != null) {
            acc.getCpcSketch().update(iValue);
        }
    }

    public void accumulate(CpcTableAcc acc, String iValue) {
        if (iValue != null) {
            acc.getCpcSketch().update(iValue);
        }
    }

    public void accumulate(CpcTableAcc acc, Double iValue) {
        if (iValue != null) {
            acc.getCpcSketch().update(iValue);
        }
    }

    public void merge(CpcTableAcc acc, Iterable<CpcTableAcc> it) {
        for (CpcTableAcc c : it) {
            acc.getCpcUnion().update(c.getCpcSketch());
        }
    }

    @Override
    public Double getValue(CpcMergeableUDAFunction.CpcTableAcc accumulator) {
        return accumulator.getCpcUnion().getResult().getEstimate();
    }

    public void resetAccumulator(CpcTableAcc acc) {
        super.resetAccumulator(acc);
    }
}
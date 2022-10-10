package org.apache.flink.benchmark.table.udaf;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.flink.core.function.table.distinct.HllMergeableUDAFunction;


public class HllMergeableUDAF extends HllMergeableUDAFunction<Double> {

    private static final long serialVersionUID = 1L;

    public HllMergeableUDAF() {
        super();
    }

    public HllMergeableUDAF(int lgConfigK, TgtHllType tgtHllType) {
        super(lgConfigK, tgtHllType);
    }

    @Override
    public Union createAccumulator() {
        return super.createAccumulator();
    }

    public void accumulate(Union acc, Long iValue) {
        if (iValue != null) {
            acc.update(iValue);
        }
    }

    public void accumulate(Union acc, String iValue) {
        if(iValue != null) {
            acc.update(iValue);
        }
    }

    public void accumulate(Union acc, Double iValue) {
        if(iValue != null) {
            acc.update(iValue);
        }
    }

    @Override
    public void merge(Union acc, Iterable<Union> it) {
        super.merge(acc, it);
    }

    @Override
    public Double getValue(Union accumulator) {
        return accumulator.getEstimate();
    }

    public void resetAccumulator(Union acc) {
        super.resetAccumulator(acc);
    }
}
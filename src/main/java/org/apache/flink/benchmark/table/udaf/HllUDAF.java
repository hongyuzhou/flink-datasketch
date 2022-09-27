package org.apache.flink.benchmark.table.udaf;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.flink.core.function.table.distinct.HllUDAFunction;
import org.apache.flink.core.serializer.HllTypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;


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

    public void accumulate(HllSketch acc, String iValue) {
        acc.update(iValue);
    }

    @Override
    public Double getValue(HllSketch accumulator) {
        return accumulator.getEstimate();
    }

    public void merge(HllSketch acc, Iterable<HllSketch> it) {
        System.out.println(Thread.currentThread() + " 执行merge");
        Union union = new Union(lgConfigK);
        System.out.println(Thread.currentThread() + " ACC Estimate is:" + acc.getEstimate());
        union.update(acc);
        for (HllSketch a : it) {
            System.out.println(Thread.currentThread() + " A Estimate is:" + a.getEstimate());
            union.update(a);
        }
        System.out.println(Thread.currentThread() + " union" + union.getResult(tgtHllType).getEstimate());
        acc = union.getResult(tgtHllType).copyAs(tgtHllType);
    }

    public void resetAccumulator(HllSketch acc) {
        super.resetAccumulator(acc);
    }
}
package org.apache.flink.benchmark.table.udaf;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.flink.core.function.table.distinct.HllUDAFunction;
import org.apache.flink.table.annotation.DataTypeHint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


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

    public void accumulate(HllSketch acc,
                           @DataTypeHint(value = "RAW", bridgedTo = Long.class) Long iValue) {
        System.out.println("这个");
        acc.update(iValue);
    }

    public void accumulate(HllSketch acc,
                           @DataTypeHint(value = "RAW", bridgedTo = String.class) String iValue) {
        acc.update(iValue);
    }

    public void accumulate(HllSketch acc,
                           @DataTypeHint(value = "RAW", bridgedTo = Double.class) Double iValue) {
        acc.update(iValue);
    }

    public void accumulate(HllSketch acc,
                           @DataTypeHint(value = "RAW", bridgedTo = ByteBuffer.class) ByteBuffer iValue) {
        acc.update(iValue);
    }

    @Override
    public Double getValue(HllSketch accumulator) {
        return accumulator.getEstimate();
    }


    // TODO； ACC Merge
//    public void merge(HllSketch acc, Iterable<HllSketch> it) {
//        System.out.println(Thread.currentThread() + " 执行merge");
//        Union union = new Union(lgConfigK);
//
//        System.out.println(Thread.currentThread() + " ACC Estimate is:" + acc.getEstimate());
//        union.update(acc);
//        for (HllSketch a : it) {
//            System.out.println(Thread.currentThread() + " A Estimate is:" + a.getEstimate());
//            union.update(a);
//        }
//        System.out.println(Thread.currentThread() + " union" + union.getResult(tgtHllType).getEstimate());
//
//
//        byte[] bytes = union.getResult(tgtHllType).toCompactByteArray();
//        HllSketch.wrap(Memory.wrap(bytes, ByteOrder.nativeOrder()));
//
//    }

    public void resetAccumulator(HllSketch acc) {
        super.resetAccumulator(acc);
    }
}
package org.apache.flink.core.function.aggregate.distinct.impl;


import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.function.aggregate.distinct.CpcAggregateFunction;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

public class CpcAccumulator<IN> extends CpcAggregateFunction<IN, Double> {

    private static final long serialVersionUID = 1L;

    private final FieldAccessor<IN, Object> fieldAccessor;

    private final CpcFunction updater;

    public CpcAccumulator(int pos, TypeInformation<IN> typeInfo, ExecutionConfig config) {
        super();
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = CpcFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
    }

    public CpcAccumulator(
            int pos,
            TypeInformation<IN> typeInfo,
            ExecutionConfig config,
            int lgK,
            long seed) {
        super(lgK, seed);
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = CpcFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
    }

    @Override
    public CpcSketch createAccumulator() {
        return new CpcSketch(lgK, seed);
    }

    @Override
    public CpcSketch add(IN value, CpcSketch accumulator) {
        updater.update(accumulator, fieldAccessor.get(value));
        return accumulator;
    }

    @Override
    public Double getResult(CpcSketch accumulator) {
        return accumulator.getEstimate();
    }

    @Override
    public CpcSketch merge(CpcSketch a, CpcSketch b) {
        CpcUnion union = new CpcUnion();
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}

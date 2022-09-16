package org.apache.flink.core.function.aggregate.distinct.impl;


import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.function.aggregate.distinct.HllAggregateFunction;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

public class HllAccumulator<IN> extends HllAggregateFunction<IN, Double> {

    private static final long serialVersionUID = 1L;

    private final FieldAccessor<IN, Object> fieldAccessor;

    private final HllFunction updater;

    public HllAccumulator(int pos, TypeInformation<IN> typeInfo, ExecutionConfig config) {
        super();
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
    }

    public HllAccumulator(
            int pos,
            TypeInformation<IN> typeInfo,
            ExecutionConfig config,
            int lgConfigK,
            TgtHllType tgtHllType) {
        super(lgConfigK, tgtHllType);
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
    }

    @Override
    public HllSketch createAccumulator() {
        return new HllSketch(lgConfigK, tgtHllType);
    }

    @Override
    public HllSketch add(IN value, HllSketch accumulator) {
        updater.update(accumulator, fieldAccessor.get(value));
        return accumulator;
    }

    @Override
    public Double getResult(HllSketch accumulator) {
        return accumulator.getEstimate();
    }

    @Override
    public HllSketch merge(HllSketch a, HllSketch b) {
        Union union = new Union();
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}

package org.apache.flink.core.function.aggregate.impl;


import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.function.aggregate.SketchAggregateFunction;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

import static org.apache.datasketches.hll.HllSketch.DEFAULT_LG_K;

public class HllAccumulator<IN> extends SketchAggregateFunction<IN, HllSketch, Double> {

    private static final long serialVersionUID = 1L;

    private final int lgConfigK;
    private final TgtHllType tgtHllType;
    private final FieldAccessor<IN, Object> fieldAccessor;
    private final HllFunction updater;

    final int MIN_LOG_K = 4;
    final int MAX_LOG_K = 21;

    public HllAccumulator(int pos, TypeInformation<IN> typeInfo, ExecutionConfig config) {
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());

        this.lgConfigK = DEFAULT_LG_K;
        this.tgtHllType = TgtHllType.HLL_4;
    }

    public HllAccumulator(
            int pos,
            TypeInformation<IN> typeInfo,
            ExecutionConfig config,
            int lgConfigK,
            TgtHllType tgtHllType) {
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());

        if ((lgConfigK >= MIN_LOG_K) && (lgConfigK <= MAX_LOG_K)) {
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

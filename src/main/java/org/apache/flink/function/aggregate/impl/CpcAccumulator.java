package org.apache.flink.function.aggregate.impl;


import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.function.aggregate.SketchAggregateFunction;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.cpc.CpcSketch.DEFAULT_LG_K;

@Internal
public class CpcAccumulator<IN> extends SketchAggregateFunction<IN, CpcSketch, Double> {

    private static final long serialVersionUID = 1L;

    private final int lgK;
    private final long seed;
    private final FieldAccessor<IN, Object> fieldAccessor;
    private final CpcFunction updater;

    public CpcAccumulator(int pos, TypeInformation<IN> typeInfo, ExecutionConfig config) {
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = CpcFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
        this.lgK = DEFAULT_LG_K;
        this.seed = DEFAULT_UPDATE_SEED;
    }

    public CpcAccumulator(
            int pos,
            TypeInformation<IN> typeInfo,
            ExecutionConfig config,
            int lgK,
            long seed) {
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = CpcFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
        this.lgK = lgK;
        this.seed = seed;
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

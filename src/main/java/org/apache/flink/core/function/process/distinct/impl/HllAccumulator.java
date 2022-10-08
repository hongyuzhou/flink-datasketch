package org.apache.flink.core.function.process.distinct.impl;


import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.function.aggregate.distinct.impl.HllFunction;
import org.apache.flink.core.function.process.distinct.HllKeyedProcessFunction;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;
import org.apache.flink.util.Collector;

public class HllAccumulator<K, IN> extends HllKeyedProcessFunction<K, IN, Double> {

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
            TgtHllType tgtHllType,
            boolean selfTypeSerializer) {
        super(lgConfigK, tgtHllType, selfTypeSerializer);
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        updater = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
    }


    @Override
    public void processElement(IN value, Context ctx, Collector<Double> out) throws Exception {
        HllSketch sketch = hll.value();

        if (sketch == null) {
            sketch = new HllSketch(lgConfigK, tgtHllType);
        }

        updater.update(sketch, fieldAccessor.get(value));
        pvCountInc();
        hll.update(sketch);

        out.collect(sketch.getEstimate());
    }
}

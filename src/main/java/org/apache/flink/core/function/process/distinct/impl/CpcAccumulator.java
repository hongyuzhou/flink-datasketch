package org.apache.flink.core.function.process.distinct.impl;


import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.function.aggregate.distinct.impl.CpcFunction;
import org.apache.flink.core.function.process.distinct.CpcKeyedProcessFunction;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;
import org.apache.flink.util.Collector;

public class CpcAccumulator<K, IN> extends CpcKeyedProcessFunction<K, IN, SketchRecord<IN>> {

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
    public void processElement(IN value, Context ctx, Collector<SketchRecord<IN>> out) throws Exception {
        CpcSketch sketch = cpc.value();

        if (sketch == null) {
            sketch = new CpcSketch(lgK, seed);
        }

        updater.update(sketch, fieldAccessor.get(value));
        pvCountInc();
        cpc.update(sketch);

        out.collect(new SketchRecord<>(value, sketch.getEstimate()));
    }
}

package org.apache.flink.core.function.process.frequencies.impl;


import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.function.process.distinct.impl.SketchRecord;
import org.apache.flink.core.function.process.frequencies.ItemsKeyedProcessFunction;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;
import org.apache.flink.util.Collector;

public class ItemsAccumulator<K, IN> extends ItemsKeyedProcessFunction<K, IN, SketchRecord<IN>> {

    private static final long serialVersionUID = 1L;

    private final FieldAccessor<IN, Object> fieldAccessor;

    //private final HllFunction updater;

    public ItemsAccumulator(int pos, TypeInformation<IN> typeInfo, ExecutionConfig config, int maxMapSize) {
        super(maxMapSize);
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        //updater = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
    }


    @Override
    public void processElement(IN value, Context ctx, Collector<SketchRecord<IN>> out) throws Exception {
        ItemsSketch<?> sketch = items.value();

        if (sketch == null) {
            sketch = new ItemsSketch(maxMapSize);
        }

        //updater.update(sketch, fieldAccessor.get(value));
        pvCountInc();
        items.update(sketch);

        //out.collect(new SketchRecord<>(value, sketch.getEstimate()));
    }
}

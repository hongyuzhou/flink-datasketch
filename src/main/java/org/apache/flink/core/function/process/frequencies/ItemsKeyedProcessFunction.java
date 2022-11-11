package org.apache.flink.core.function.process.frequencies;

import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.function.process.SketchKeyedProcessFunction;
import org.apache.flink.core.serializer.ItemsTypeSerializer;

/**
 * Use ItemsSketch {@link ItemsSketch} For ItemsKeyedProcessFunction
 *
 * @param <K> Type of the key.
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
public abstract class ItemsKeyedProcessFunction<K, I, O> extends SketchKeyedProcessFunction<K, I, O> {

    private static final long serialVersionUID = 1L;

    protected ValueState<ItemsSketch> items;

    protected int maxMapSize;

    public ItemsKeyedProcessFunction(int maxMapSize) {
        this.maxMapSize = maxMapSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<ItemsSketch> itemSketchStateDescriptor = new ValueStateDescriptor<>("itemsSketchState",
                new ItemsTypeSerializer(ItemsSketch.class, getRuntimeContext().getExecutionConfig()));
        items = getRuntimeContext().getState(itemSketchStateDescriptor);
    }
}

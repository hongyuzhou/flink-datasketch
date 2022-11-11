package org.apache.flink.core.serializer;

import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

/**
 * A serializer for {@link ItemsSketch}
 * ItemsTypeSerializer Depended on KryoSerializer
 */
public class ItemsTypeSerializer extends KryoSerializer<ItemsSketch> {

    private static final long serialVersionUID = 1L;

    public ItemsTypeSerializer(Class<ItemsSketch> type, ExecutionConfig executionConfig) {
        super(type, executionConfig);
    }
}

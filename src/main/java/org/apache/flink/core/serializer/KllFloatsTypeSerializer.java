package org.apache.flink.core.serializer;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

/**
 * A serializer for {@link KllFloatsSketch}
 * KllFloatsTypeSerializer Depended on KryoSerializer
 */
public class KllFloatsTypeSerializer extends KryoSerializer<KllFloatsSketch> {

    private static final long serialVersionUID = 1L;

    public KllFloatsTypeSerializer(Class<KllFloatsSketch> type, ExecutionConfig executionConfig) {
        super(type, executionConfig);
    }
}

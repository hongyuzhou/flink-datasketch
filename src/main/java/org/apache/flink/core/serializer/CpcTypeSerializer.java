package org.apache.flink.core.serializer;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

public class CpcTypeSerializer extends KryoSerializer<CpcSketch> {

    private static final long serialVersionUID = 1L;

    public CpcTypeSerializer(Class<CpcSketch> type, ExecutionConfig executionConfig) {
        super(type, executionConfig);
    }
}

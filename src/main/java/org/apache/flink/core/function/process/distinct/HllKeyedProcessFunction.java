package org.apache.flink.core.function.process.distinct;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.core.serializer.HllTypeSerializer;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.function.process.SketchKeyedProcessFunction;

import static org.apache.datasketches.hll.HllSketch.DEFAULT_LG_K;

public abstract class HllKeyedProcessFunction<K, I, O> extends SketchKeyedProcessFunction<K, I, O> {

    private static final long serialVersionUID = 1L;

    protected ValueState<HllSketch> hll;

    protected int lgConfigK;

    protected TgtHllType tgtHllType;

    protected boolean selfTypeSerializer;

    public HllKeyedProcessFunction() {
        this.lgConfigK = DEFAULT_LG_K;
        this.tgtHllType = TgtHllType.HLL_4;
        this.selfTypeSerializer = false;
    }

    public HllKeyedProcessFunction(int lgConfigK, TgtHllType tgtHllType, boolean selfTypeSerializer) {
        if ((lgConfigK < 4) || (lgConfigK > 21)) {
            throw new SketchesArgumentException(
                    "Log K must be between 4 and 21, inclusive: " + lgConfigK);
        }
        this.lgConfigK = lgConfigK;
        this.tgtHllType = tgtHllType;
        this.selfTypeSerializer = selfTypeSerializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<HllSketch> hllSketchStateDescriptor = new ValueStateDescriptor<>("HllSketchState",
                selfTypeSerializer ? new HllTypeSerializer() :
                        new KryoSerializer<>(HllSketch.class, getRuntimeContext().getExecutionConfig()));
        hll = getRuntimeContext().getState(hllSketchStateDescriptor);
    }
}

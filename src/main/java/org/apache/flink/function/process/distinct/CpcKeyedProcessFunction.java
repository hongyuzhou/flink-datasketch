package org.apache.flink.function.process.distinct;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.function.process.SketchKeyedProcessFunction;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.hll.HllSketch.DEFAULT_LG_K;

public abstract class CpcKeyedProcessFunction<K, I, O> extends SketchKeyedProcessFunction<K, I, O> {

    private static final long serialVersionUID = 1L;

    protected ValueState<CpcSketch> cpc;

    protected int lgK;
    protected long seed;

    public CpcKeyedProcessFunction() {
        this.lgK = DEFAULT_LG_K;
        this.seed = DEFAULT_UPDATE_SEED;
    }

    public CpcKeyedProcessFunction(int lgK, long seed) {
        if ((lgK < 4) || (lgK > 26)) {
            throw new SketchesArgumentException("LgK must be >= 4 and <= 26: " + lgK);
        }
        this.lgK = lgK;
        this.seed = seed;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<CpcSketch> cpcSketchStateDescriptor = new ValueStateDescriptor<>("CpcSketchState",
                new KryoSerializer<>(CpcSketch.class, getRuntimeContext().getExecutionConfig()));
        cpc = getRuntimeContext().getState(cpcSketchStateDescriptor);
    }
}

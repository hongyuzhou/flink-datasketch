package org.apache.flink.benchmark.operator.process;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.function.process.distinct.HllKeyedProcessFunction;
import org.apache.flink.util.Collector;

public class HllKeyedProcess extends HllKeyedProcessFunction<String, Tuple3<String, Long, String>, Double> {

    private static final long serialVersionUID = 1L;

    public HllKeyedProcess() {
        super();
    }

    public HllKeyedProcess(int lgConfigK, TgtHllType tgtHllType, boolean selfTypeSerializer) {
        super(lgConfigK, tgtHllType, selfTypeSerializer);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(Tuple3<String, Long, String> value, Context ctx, Collector<Double> out) throws Exception {
        HllSketch sketch = hll.value();

        if (sketch == null) {
            sketch = new HllSketch(lgConfigK, tgtHllType);
        }

        sketch.update(value.f2);
        pvCountInc();
        hll.update(sketch);

        out.collect(sketch.getEstimate());
    }
}

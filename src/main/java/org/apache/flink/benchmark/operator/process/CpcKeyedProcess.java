package org.apache.flink.benchmark.operator.process;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.function.process.distinct.CpcKeyedProcessFunction;
import org.apache.flink.core.function.process.distinct.impl.SketchRecord;
import org.apache.flink.util.Collector;

public class CpcKeyedProcess extends CpcKeyedProcessFunction<String, Tuple3<String, Long, String>, SketchRecord<Tuple3<String, Long, String>>> {

    private static final long serialVersionUID = 1L;

    public CpcKeyedProcess() {
        super();
    }

    public CpcKeyedProcess(int lgK, long seed) {
        super(lgK, seed);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(Tuple3<String, Long, String> value, Context ctx, Collector<SketchRecord<Tuple3<String, Long, String>>> out) throws Exception {
        CpcSketch sketch = cpc.value();

        if (sketch == null) {
            sketch = new CpcSketch(lgK, seed);
        }

        sketch.update(value.f2);
        pvCountInc();
        cpc.update(sketch);

        out.collect(new SketchRecord<>(value, sketch.getEstimate()));
    }
}

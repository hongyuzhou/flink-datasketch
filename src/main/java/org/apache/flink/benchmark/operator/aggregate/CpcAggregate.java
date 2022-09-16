package org.apache.flink.benchmark.operator.aggregate;

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.function.aggregate.SketchAggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Set;

public class CpcAggregate extends SketchAggregateFunction<Tuple3<String, Long, String>, Tuple3<CpcSketch, Long, Set<String>>, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(CpcAggregate.class);

    @Override
    public Tuple3<CpcSketch, Long, Set<String>> createAccumulator() {
        return Tuple3.of(
                new CpcSketch(),
                0L,
                null);
    }

    @Override
    public Tuple3<CpcSketch, Long, Set<String>> add(
            Tuple3<String, Long, String> value,
            Tuple3<CpcSketch, Long, Set<String>> accumulator) {
        accumulator.f0.update(value.f2);
        accumulator.f1 += 1;
        //accumulator.f2.add(value.f2);
        return Tuple3.of(
                accumulator.f0,
                accumulator.f1,
                accumulator.f2);
    }

    @Override
    public Double getResult(Tuple3<CpcSketch, Long, Set<String>> accumulator) {
        LOG.info(
                "Count is: {}, Estimate is: {} "
                        + "CpcSketch Size is: {} byte, Accuracy Rate is: {}%",
                accumulator.f1,
                accumulator.f0.getEstimate(),
                //accumulator.f2.size(),
                //0,
                accumulator.f0.toByteArray().length,
                new BigDecimal(100
                        - Math.abs(accumulator.f0.getEstimate() - accumulator.f1)
                        / accumulator.f1 * 100).setScale(
                        2,
                        BigDecimal.ROUND_HALF_UP));
        return accumulator.f0.getEstimate();
    }

    @Override
    public Tuple3<CpcSketch, Long, Set<String>> merge(
            Tuple3<CpcSketch, Long, Set<String>> a,
            Tuple3<CpcSketch, Long, Set<String>> b) {
        return null;
    }
}

package org.apache.flink.benchmark.operator.aggregate;

import org.apache.datasketches.hll.HllSketch;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.function.aggregate.SketchAggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Implementation of SketchAggregateFunction
 */
public class HllAggregate extends SketchAggregateFunction<Tuple3<String, Long, String>, Tuple3<HllSketch, Long, Set<String>>, Double> {

    private static final Logger LOG = LoggerFactory.getLogger(HllAggregate.class);

    @Override
    public Tuple3<HllSketch, Long, Set<String>> createAccumulator() {
        return Tuple3.of(
                new HllSketch(),
                0L,
                null);
    }

    @Override
    public Tuple3<HllSketch, Long, Set<String>> add(
            Tuple3<String, Long, String> value,
            Tuple3<HllSketch, Long, Set<String>> accumulator) {
        accumulator.f0.update(value.f2);
        accumulator.f1 += 1;
        //accumulator.f2.add(value.f2);
        return Tuple3.of(
                accumulator.f0,
                accumulator.f1,
                accumulator.f2);
    }

    @Override
    public Double getResult(Tuple3<HllSketch, Long, Set<String>> accumulator) {
        LOG.info(
                "Count is: {}, Estimate is: {} "
                        + "HllSketch Size is: {} byte, Accuracy Rate is: {}%",
                accumulator.f1,
                accumulator.f0.getEstimate(),
                //accumulator.f2.size(),
                //0,
                accumulator.f0.getUpdatableSerializationBytes(),
                new BigDecimal(100
                        - Math.abs(accumulator.f0.getEstimate() - accumulator.f1)
                        / accumulator.f1 * 100).setScale(
                        2,
                        BigDecimal.ROUND_HALF_UP));
        return accumulator.f0.getEstimate();
    }

    @Override
    public Tuple3<HllSketch, Long, Set<String>> merge(
            Tuple3<HllSketch, Long, Set<String>> a,
            Tuple3<HllSketch, Long, Set<String>> b) {
        return null;
    }
}

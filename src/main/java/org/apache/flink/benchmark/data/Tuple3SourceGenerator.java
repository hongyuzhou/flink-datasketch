package org.apache.flink.benchmark.data;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.AbstractID;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Random generator Tuple3 Test Data
 * <city, event_time, value>
 */
public class Tuple3SourceGenerator extends RandomGenerator<Tuple3<String, Long, String>> {

    private final List<String> city = Collections.unmodifiableList(Arrays.asList(
            "A", "B", "C", "D",
            "E", "F", "G", "H",
            "I", "J", "K", "L",
            "M", "N", "O", "P",
            "Q", "R", "S", "T",
            "U", "V", "W", "X"));

    @Override
    public Tuple3<String, Long, String> next() {
        AbstractID id = new AbstractID();

        return Tuple3.of(
                city.get(random.nextInt(0, city.size() - 1)),
                System.currentTimeMillis(),
                id.toHexString());
    }
}

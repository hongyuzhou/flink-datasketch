package org.apache.flink.benchmark.data;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Shakespeare DataGenerator
 */
public class ShakespeareDataGenerator implements DataGenerator<String> {

    private static final Logger LOG = LoggerFactory.getLogger(ShakespeareDataGenerator.class);

    protected transient BufferedReader reader;

    protected transient String line;

    @Override
    public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        int parallelism = runtimeContext.getNumberOfParallelSubtasks();
        if (parallelism > 1) {
            throw new IllegalArgumentException("ShakespeareDataGenerator Only Allow 1 parallelism");
        }
        reader = new BufferedReader(new InputStreamReader(new FileInputStream("src/main/resources/data/shakespeare.txt")));
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = true;

        try {
            line = reader.readLine();
            hasNext = (line != null);
        } catch (IOException e) {
            LOG.error("BufferedReader Read Line Error", e);
        }

        if (!hasNext) {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                LOG.error("BufferedReader Close Failed", e);
            }
        }
        return hasNext;
    }

    @Override
    public String next() {
        return line;
    }
}

package core.data;

import org.apache.flink.benchmark.data.ShakespeareDataGenerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.junit.Test;

public class Shakespeare {

    @Test
    public void ShakespeareDataSourceItCase() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.addSource(new DataGeneratorSource<>(new ShakespeareDataGenerator(), 10, null))
                .setParallelism(1)
                .returns(String.class)
                .print()
                .setParallelism(1);
        env.execute();
    }
}

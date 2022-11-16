package org.apache.flink.benchmark.table;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class TableSketchBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(TableSketchBenchMark.class);

    private final String name;
    private final String sqlQuery;
    private final int loopNum;
    private final TableEnvironment tEnv;

    public TableSketchBenchMark(String name, String sqlQuery, int loopNum, TableEnvironment tEnv) {
        this.name = name;
        this.sqlQuery = sqlQuery;
        this.loopNum = loopNum;
        this.tEnv = tEnv;
    }

    public void run(List<Tuple2<String, Long>> bestArray) throws Exception {
        List<Result> results = new ArrayList<>();
        for (int i = 0; i < loopNum; i++) {
            System.err.printf("--------------- Running %s %s/%s ---------------%n", name, (i + 1), loopNum);
            results.add(runInternal(i));
        }
        printResults(results, bestArray);
    }

    private Result runInternal(int loop) throws Exception {
        System.gc();

        LOG.info("begin register tables.");

        long start = System.currentTimeMillis();
        LOG.info(" begin optimize.");

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tEnv.getConfig().getConfiguration().setString(PipelineOptions.NAME, name + "-" + (loop + 1) + "/" + loopNum);

        Table table = tEnv.sqlQuery(sqlQuery);

        LOG.info(" begin execute.");

        table.execute().print();

        LOG.info(" end execute.");

        long totalTime = System.currentTimeMillis() - start;

        return new Result(totalTime);
    }

    private void printResults(List<Result> results, List<Tuple2<String, Long>> bestArray) throws Exception {
        int itemMaxLength = 20;
        System.err.println();
        printLine('-', "+", itemMaxLength, "", "", "", "");
        printLine(' ', "|", itemMaxLength, " " + name, " Best Time(ms)", " Avg Time(ms)", " Max Time(ms)");
        printLine('-', "+", itemMaxLength, "", "", "", "");

        Tuple3<Long, Long, Long> t3 = getBestAvgMaxTime(results, "getTotalTime");
        printLine(' ', "|", itemMaxLength, " Total", " " + t3.f0, " " + t3.f1, " " + t3.f2);
        printLine('-', "+", itemMaxLength, "", "", "", "");
        bestArray.add(new Tuple2<>(name, t3.f0));
        System.err.println();
    }

    private void printLine(char charToFill, String separator, int itemMaxLength, String... items) {
        StringBuilder builder = new StringBuilder();
        for (String item : items) {
            builder.append(separator);
            builder.append(item);
            int left = itemMaxLength - item.length() - separator.length();
            for (int i = 0; i < left; ++i) {
                builder.append(charToFill);
            }
        }
        builder.append(separator);
        System.err.println(builder.toString());
    }

    private Tuple3<Long, Long, Long> getBestAvgMaxTime(List<Result> results, String methodName) throws Exception {
        long best = Long.MAX_VALUE;
        long sum = 0L;
        long max = Long.MIN_VALUE;
        Method method = Result.class.getMethod(methodName);
        for (Result result : results) {
            long time = (long) method.invoke(result);
            if (time < best) {
                best = time;
            }
            sum += time;
            if (time > max) {
                max = time;
            }
        }
        return new Tuple3<>(best, sum / results.size(), max);
    }

    private void printRow(List<Row> rowList) {
        for (Row row : rowList) {
            System.out.println(row);
        }
    }

    /**
     * 查询执行评估结果
     */
    private class Result {
        private final long totalTime;

        private Result(long totalTime) {
            this.totalTime = totalTime;
        }

        public long getTotalTime() {
            return totalTime;
        }
    }

}

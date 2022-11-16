package org.apache.flink.benchmark.table.asserts;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * TableAccuracyAssert
 */
public class TableAccuracyAssert {

    private static final Logger LOG = LoggerFactory.getLogger(TableAccuracyAssert.class);

    private final String name;
    private final String originSqlQuery;
    private final String sketchSqlQuery;
    private final int loopNum;
    private final TableEnvironment tEnv;

    public TableAccuracyAssert(String name, String originSqlQuery, String sketchSqlQuery, int loopNum, TableEnvironment tEnv) {
        this.name = name;
        this.originSqlQuery = originSqlQuery;
        this.sketchSqlQuery = sketchSqlQuery;
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

        LOG.info(" begin optimize.");

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        tEnv.getConfig().getConfiguration().setString(PipelineOptions.NAME, name + "-" + (loop + 1) + "/" + loopNum);

        Table expected = tEnv.sqlQuery(originSqlQuery);

        Table sketch = tEnv.sqlQuery(sketchSqlQuery);

        LOG.info("origin & sketch begin execute.");

        Result result = batchAccuracyAssert("", "uniq_customer_cnt", expected, sketch);

        LOG.info(" accuracy assert end execute.");
        return result;
    }


    /**
     * Table SQL Accuracy Assert Logic
     *
     * @param joinKey
     * @param metric
     * @param expected
     * @param actual
     * @return
     */
    public Result batchAccuracyAssert(String joinKey, String metric, Table expected, Table actual) {

        tEnv.createTemporaryView("expected", expected);
        tEnv.createTemporaryView("actual", actual);

        StringBuilder joinExpr = new StringBuilder();
        if (StringUtils.isEmpty(joinKey)) {
            joinExpr.append(" 1 = 1");
        } else {
            String[] joinKeys = joinKey.split(",");
            String joinPattern = " e.%s = a.%s";
            for (int i = 0; i < joinKeys.length; i++) {
                if (i > 0) {
                    joinExpr.append(" and");
                }
                joinExpr.append(String.format(joinPattern, joinKeys[i], joinKeys[i]));
            }
        }

        CloseableIterator<Row> iterator = tEnv.sqlQuery(String.format(
                "select " +
                        " cast(avg(e.%s) as bigint)                                        as avg_cardinality, " +
                        " round(avg(abs(e.%s - nullif(a.estimate_%s,0)) / e.%s) * 100,2)   as avg_error_rate " +
                        "from expected e " +
                        "left join actual a " +
                        " on %s", metric, metric, metric, metric, joinExpr.toString()))
                .execute()
                .collect();

        tEnv.dropTemporaryView("expected");
        tEnv.dropTemporaryView("actual");

        // 保证iterator 的大小是1
        Row row = iterator.next();
        return new Result((long) row.getField(0), (double) row.getField(1));
    }

    private void printResults(List<Result> results, List<Tuple2<String, Long>> bestArray) throws Exception {
        int itemMaxLength = 25;
        System.err.println();
        printLine('-', "+", itemMaxLength, "", "", "", "", "");
        printLine(' ', "|", itemMaxLength, " " + name, " Avg Cardinality", " Min AvgErrRate(%)", " Max AvgErrRate(%)", " Stddev AvgErrRate");
        printLine('-', "+", itemMaxLength, "", "", "", "", "");

        Tuple4<Long, Double, Double, Double> t4 = getMinStdMaxAvgErrorRate(results);
        printLine(' ', "|", itemMaxLength, " Total", " " + t4.f0, " " + t4.f1, " " + t4.f2, " " + String.format("%.4f", t4.f3));
        printLine('-', "+", itemMaxLength, "", "", "", "", "");
        bestArray.add(new Tuple2<>(name, t4.f0));
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

    /**
     * 获得测试数据
     *
     * @param results
     * @return
     * @throws Exception
     */
    private Tuple4<Long, Double, Double, Double> getMinStdMaxAvgErrorRate(List<Result> results) throws Exception {
        double min = Double.MAX_VALUE;
        double sum = 0L;
        double[] std = new double[results.size()];
        double max = Double.MIN_VALUE;
        long cardinality = results.get(0).getAvgCardinality();

        int i = 0;
        for (Result result : results) {
            double errorRate = result.getAvgErrorRate();
            std[i] = errorRate;
            if (errorRate < min) {
                min = errorRate;
            }
            sum += errorRate;
            if (errorRate > max) {
                max = errorRate;
            }
            i++;
        }

        double stdVal = stddevSample(std, sum / results.size());
        return new Tuple4<>(cardinality, min, max, stdVal);
    }

    /**
     * stddev_sample function
     *
     * @param data
     * @param avg
     * @return
     */
    private double stddevSample(double[] data, double avg) {
        double variance = 0.0d;
        for (int i = 0; i < data.length; i++) {
            variance += (Math.pow(data[i] - avg, 2));
        }
        variance = variance / (data.length - 1);
        return variance;
    }

    /**
     * 查询准确性执行评估结果
     */
    private class Result {
        private final long avgCardinality;
        private final double avgErrorRate;

        public Result(long avgCardinality, double avgErrorRate) {
            this.avgCardinality = avgCardinality;
            this.avgErrorRate = avgErrorRate;
        }

        public long getAvgCardinality() {
            return avgCardinality;
        }

        public double getAvgErrorRate() {
            return avgErrorRate;
        }
    }
}

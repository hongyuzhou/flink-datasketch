package org.apache.flink.benchmark.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.benchmark.table.udaf.HllUDAF;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueryTableSketchBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(QueryTableSketchBenchMark.class);

    public static void main(String[] args) throws Exception {
        TableEnvironment tEnv = setUpEnv();

        List<Tuple2<String, Long>> bestArray = new ArrayList<>();
        runQuery(tEnv, "X", 20, bestArray);
    }

    private static TableEnvironment setUpEnv() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig().getConfiguration().setBoolean(
                OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true
        );
        tEnv.getConfig().getConfiguration().setBoolean(
                OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true
        );
        tEnv.getConfig().getConfiguration().setBoolean(
                OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, true
        );
        tEnv.getConfig().getConfiguration().setString(
                ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, ShuffleMode.BATCH.toString()
        );

        setUpTables(tEnv);
        return tEnv;
    }

    private static void setUpTables(TableEnvironment tEnv) {

    }

    private static void runQuery(TableEnvironment tEnv, String queryName, int loopNum, List<Tuple2<String, Long>> bestArray) throws Exception {
        String queryString = fileToString(new File(""));
        TableSketchBenchMark benchMark = new TableSketchBenchMark(queryName, queryString, loopNum, tEnv);
        benchMark.run(bestArray);
    }

    private static String fileToString(File file) {

        FileInputStream inStream = null;
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();

        try {
            inStream = new FileInputStream(file);
            int str;
            while ((str = inStream.read()) != -1) {
                outStream.write(str);
            }
            outStream.flush();
        } catch (IOException e) {
            LOG.error("Query SQL File Error", e);
        } finally {
            try {
                Objects.requireNonNull(inStream).close();
            } catch (IOException e) {
                LOG.error("Query SQL File Error", e);
            }
        }

        try {
            return outStream.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.error("SQL 2 String Error", e);
        }
        return "";
    }


//    public static void main(String[] args) {
//        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//        TableEnvironment tEnv = TableEnvironment.create(bbSettings);
//
//
//        final Table rawCustomers =
//                tEnv.fromValues(
//                        Row.of(
//                                "A",
//                                System.currentTimeMillis(),
//                                new AbstractID().toHexString()),
//                        Row.of(
//                                "B",
//                                System.currentTimeMillis(),
//                                new AbstractID().toHexString()),
//                        Row.of(
//                                "C",
//                                System.currentTimeMillis(),
//                                new AbstractID().toHexString()),
//                        Row.of(
//                                "D",
//                                System.currentTimeMillis(),
//                                new AbstractID().toHexString()),
//                        Row.of(
//                                "A",
//                                System.currentTimeMillis(),
//                                new AbstractID().toHexString()),
//                        Row.of(
//                                "A",
//                                System.currentTimeMillis(),
//                                new AbstractID().toHexString()))
//                        .as("city", "event_time", "uniq_key");
//
//        tEnv.createTemporarySystemFunction("hll", new HllUDAF());
//
////        Table estimate = rawCustomers
////                .groupBy($("city"))
////                .select($("city"), call("hll", $("uniq_key")).as("estimate"));
//
//
//        tEnv.createTemporaryView("rawCustomers", rawCustomers);
//        Table estimate = tEnv.sqlQuery(
//                "SELECT city, hll(uniq_key) as estimate FROM rawCustomers GROUP BY city"
//        );
//
//        //estimate.execute().collect().forEachRemaining(res::add);
//
//        //res.execute().print();
//    }
}

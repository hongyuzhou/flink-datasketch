package org.apache.flink.benchmark.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.benchmark.table.udaf.*;
import org.apache.flink.benchmark.table.udaf.FreqItemsUDAF;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Benchmark For Table/SQL
 */
public class QueryTableSketchBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(QueryTableSketchBenchMark.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        if (!params.has("dataPath")) {
            throw new IllegalArgumentException("Must Use --dataPath to specify data path.");
        }
        String dataPath = params.get("dataPath");

        int loopNum = params.getInt("loopNum", 3);

        String queryType = params.get("queryType", "distinct");

        TableEnvironment tEnv = setUpEnv(dataPath);

        List<Tuple2<String, Long>> bestArray = new ArrayList<>();
        String sqlIdx = params.get("sqlIdx", "-1");
        if ("-1".equals(sqlIdx)) {
            for (int i = 0; i < 15; i++) {
                runQuery(tEnv, queryType, "query" + i + ".sql", loopNum, bestArray);
            }
        } else {
            runQuery(tEnv, queryType, "query" + sqlIdx + ".sql", loopNum, bestArray);
        }

    }

    private static TableEnvironment setUpEnv(String dataPath) {
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
        tEnv.getConfig().getConfiguration().setBoolean(
                OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true
        );

        setUpTables(tEnv, dataPath);
        return tEnv;
    }

    private static void setUpTables(TableEnvironment tEnv, String dataPath) {
        String ddl = "" +
                "CREATE TEMPORARY TABLE store_sales (" +
                "   ss_sold_date_sk BIGINT," +
                "   ss_sold_time_sk BIGINT," +
                "   ss_item_sk BIGINT," +
                "   ss_customer_sk BIGINT," +
                "   ss_cdemo_sk BIGINT," +
                "   ss_hdemo_sk BIGINT," +
                "   ss_addr_sk BIGINT," +
                "   ss_store_sk BIGINT," +
                "   ss_promo_sk BIGINT," +
                "   ss_ticket_number BIGINT," +
                "   ss_quantity BIGINT," +
                "   ss_wholesale_cost DECIMAL(7,2)," +
                "   ss_list_price DECIMAL(7,2)," +
                "   ss_sales_price DECIMAL(7,2)," +
                "   ss_ext_discount_amt DECIMAL(7,2)," +
                "   ss_ext_sales_price DECIMAL(7,2)," +
                "   ss_ext_wholesale_cost DECIMAL(7,2)," +
                "   ss_ext_list_price DECIMAL(7,2)," +
                "   ss_ext_tax DECIMAL(7,2)," +
                "   ss_coupon_amt DECIMAL(7,2)," +
                "   ss_net_paid DECIMAL(7,2)," +
                "   ss_net_paid_inc_tax DECIMAL(7,2)," +
                "   ss_net_profit DECIMAL(7,2)" +
                ") " +
                "WITH (" +
                "   'connector' = 'filesystem', \n" +
                "   'path' = 'file://%s', \n" +
                "   'format' = 'csv', \n" +
                "   'csv.field-delimiter' = '|', \n" +
                "   'csv.null-literal' = 'true', \n" +
                "   'csv.ignore-parse-errors' = 'true'" +
                ")";
        tEnv.executeSql(String.format(ddl, dataPath));
        //tEnv.executeSql("select * from store_sales limit 10").print();
        tEnv.createTemporarySystemFunction("hll", new HllUDAF());
        tEnv.createTemporarySystemFunction("cpc", new CpcUDAF());
        tEnv.createTemporarySystemFunction("hll_merge", new HllMergeableUDAF());
        tEnv.createTemporarySystemFunction("cpc_merge", new CpcMergeableUDAF());
        tEnv.createTemporarySystemFunction("frequencies_items", new FreqItemsUDAF(64, 10));
    }

    private static void runQuery(TableEnvironment tEnv, String queryType, String queryName, int loopNum, List<Tuple2<String, Long>> bestArray) throws Exception {
        InputStream inStream =
                Objects.requireNonNull(QueryTableSketchBenchMark.class.getClassLoader().getResourceAsStream(String.format("table/queries/%s/%s", queryType, queryName)));
        String queryString = fileToString(inStream);
        TableSketchBenchMark benchMark = new TableSketchBenchMark(queryName, queryString, loopNum, tEnv);
        benchMark.run(bestArray);
    }

    private static String fileToString(InputStream inStream) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();

        try {
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
            LOG.error("SQL to String Error", e);
        }
        return "";
    }
}

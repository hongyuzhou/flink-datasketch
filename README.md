# Flink-Datasketches
## Apache-Flink &amp; Apache-Datasketches
0. Maven

`mvn clean package`


1. Stream API Benchmark 执行

`./bin/flink run -c org.apache.flink.benchmark.basics.WithoutSketchBenchMark ${your_path}/flink-datasketches-1.0-SNAPSHOT.jar`


2. Batch SQL Benchmark 执行

* 下载tpcds数据集工具tpcds-kit <br>
* 进入`tpcds-kit/tools`目录, 执行 `./dsdgen -scale 1 -dir ${your_tpc_data_path} -table store_sales`  生成store_sales表数据 <br>
* 命令执行 <br>
`./bin/flink run ${your_path}/flink-datasketches-1.0-SNAPSHOT.jar --dataPath ${your_tpc_data_path}/store_sales.dat`

# Flink-Datasketches
## Apache-Flink &amp; Apache-Datasketches

### Flink基于[Apache DataSketces](https://datasketches.apache.org) 快速计算不可扩展（don’t scale）的非累加指标解决方案

#### Abstract
开源组件Apache DataSketches在不可扩展（don’t scale）的非累加指标计算中有非常优秀表现，基于此考虑将其结合到Flink的实际应用中。在大数量集实际生产中，实现更快，更少的计算资源消耗，获得业务可以接受的计算结果。

#### Benchmark-Test

##### 0. Maven 打包

`mvn clean package`


##### 1. Stream API Benchmark 执行

`./bin/flink run -c org.apache.flink.benchmark.basics.WithoutSketchBenchMark ${your_path}/flink-datasketches-1.0-SNAPSHOT.jar`


##### 2. Batch SQL Benchmark 执行

* 下载tpcds数据集工具`tpcds-kit` <br>
* 进入`tpcds-kit/tools`目录, 执行 `./dsdgen -scale 1 -dir ${your_tpc_data_path} -table store_sales`  生成store_sales表数据 <br>
* 命令执行 <br>
`./bin/flink run ${your_path}/flink-datasketches-1.0-SNAPSHOT.jar --dataPath ${your_tpc_data_path}/store_sales.dat`

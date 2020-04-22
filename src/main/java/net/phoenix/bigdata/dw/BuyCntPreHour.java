package net.phoenix.bigdata.dw;

import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class BuyCntPreHour {

    public static void main(String[] args) throws  Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        //设置检查点
        fsEnv.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //注册订单明细表
        String source_table_name = "dwd_kafka_orders";
        String dwd_kafka_orders = "CREATE TABLE "+source_table_name+" (\n" +
                "    orderId BIGINT," +
                "    orderNo STRING," +
                "    userId BIGINT," +
                "    goodId BIGINT," +
                "    goodsMoney DOUBLE," +
                "    realTotalMoney DOUBLE," +
                "    payFrom BIGINT," +
                "    province STRING," +
                "    createTime TIMESTAMP(3)," +
                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "    WATERMARK FOR createTime as createTime - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                ") WITH (" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.properties.group.id' = 'dwd_kafka_orders_group1',"+
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = '"+source_table_name+"',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'latest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")";
        tableEnv.sqlUpdate(dwd_kafka_orders);

        //注册每小时订单kafka结果表
        String kafka_sink_table = "dws_kafka_orders_per_hours";
        String kafkaSourceSQL = "CREATE TABLE " + kafka_sink_table + "(" +
                "    day_hour_time STRING," +
                "    order_num BIGINT" +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'update-mode' = 'append',"+
                "    'connector.properties.group.id' = 'dws_kafka_orders_per_hours_group1',"+
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = '"+kafka_sink_table+"'," +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"'," +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"'," +
                "    'connector.startup-mode' = 'earliest-offset'," +
                "    'format.type' = 'json'" +
                ")";
        tableEnv.sqlUpdate(kafkaSourceSQL);




        //每小时订单计算逻辑生成临时表
        String resultSql="select max(substring(DATE_FORMAT(createTime,'yyyy-MM-dd HH:mm:ss'),1,13)) day_hour_time,"+
                        "   count(distinct orderId) order_num" +
                        " from  " + source_table_name+
                        " group by TUMBLE(createTime, INTERVAL '1' HOUR)";

        Table resultRs = tableEnv.sqlQuery(resultSql);
        resultRs.printSchema();
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultRs, Row.class);
        tableEnv.createTemporaryView("view_BuyCntPreHour",tuple2DataStream);

        tuple2DataStream.print();


      //每小时订单汇总结果sink到dws层kafka
        String insertSQL = "INSERT INTO "+kafka_sink_table+
                " SELECT day_hour_time,order_num  FROM view_BuyCntPreHour";
        tableEnv.sqlUpdate(insertSQL);



        //注册APP层ES结果表
        String es_rs_table = "buy_orders_per_hour";
        String es_table = "CREATE TABLE " + es_rs_table + " ( \n" +
                "    day_hour_time STRING,\n" +
                "    buy_cnt BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector\n" +
                "    'connector.version' = '7',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本\n" +
                "    'connector.hosts' = '"+GlobalConfig.ES_CONNECTOR_URL+"',  -- elasticsearch 地址\n" +
                "    'connector.index' = '"+es_rs_table+"',  -- elasticsearch 索引名，相当于数据库的表名\n" +
                "    'connector.document-type' = 'user_behavior', -- elasticsearch 的 type，相当于数据库的库名\n" +
                "    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新\n" +
                "    'format.type' = 'json',  -- 输出数据格式 json\n" +
                "    'update-mode' = 'upsert'\n" +
                ")";
        tableEnv.sqlUpdate(es_table);

        //将dws层每小时订单汇总结果 sink到APP层ES
        String insertESSQL = "INSERT INTO "+es_rs_table+
                " SELECT  day_hour_time,max(order_num)  FROM "+kafka_sink_table+
                " group by day_hour_time";
        tableEnv.sqlUpdate(insertESSQL);





        //分钟统计订单逻辑：dws分钟级结果表
        String one_minute_sink_table = "dws_kafka_orders_per_minute";
        String one_minute_sink_tableSQL = "CREATE TABLE " + one_minute_sink_table + "(" +
                "    day_time_str STRING," +
                "    order_num BIGINT" +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'update-mode' = 'append',"+
                "    'connector.properties.group.id' = 'dws_kafka_orders_per_minute_group1',"+
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = '"+one_minute_sink_table+"'," +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"'," +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"'," +
                "    'connector.startup-mode' = 'earliest-offset'," +
                "    'format.type' = 'json'" +
                ")";
        tableEnv.sqlUpdate(one_minute_sink_tableSQL);


        //1分钟统计订单逻辑
        String one_minute_resultSql="select substring(DATE_FORMAT(createTime,'yyyy-MM-dd HH:mm:ss'),1,16) day_time_str,"+
                "   count(distinct orderId) order_num" +
                " from  " + source_table_name+
                " group by TUMBLE(createTime, INTERVAL '1' MINUTE)";
        //注册成临时表
        Table table = tableEnv.sqlQuery(one_minute_resultSql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream1 = tableEnv.toRetractStream(table, Row.class);
        tableEnv.createTemporaryView("one_minute_orders_rs",tuple2DataStream1);

        //每分钟订单汇总结果sink到dws层kafka
        String insert_per_minute_SQL = "INSERT INTO "+one_minute_sink_table+
                " SELECT  day_time_str,order_num  FROM one_minute_orders_rs";

        tableEnv.sqlUpdate(insert_per_minute_SQL);


        //注册APP层ES结果表
        String es_rs_table2 = "buy_orders_per_minute";
        String es_table2 = "CREATE TABLE " + es_rs_table2 + " ( \n" +
                "    day_time_str STRING,\n" +
                "    buy_cnt BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector\n" +
                "    'connector.version' = '7',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本\n" +
                "    'connector.hosts' = '"+GlobalConfig.ES_CONNECTOR_URL+"',  -- elasticsearch 地址\n" +
                "    'connector.index' = '"+es_rs_table+"',  -- elasticsearch 索引名，相当于数据库的表名\n" +
                "    'connector.document-type' = 'user_behavior', -- elasticsearch 的 type，相当于数据库的库名\n" +
                "    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新\n" +
                "    'format.type' = 'json',  -- 输出数据格式 json\n" +
                "    'update-mode' = 'upsert'\n" +
                ")";
        tableEnv.sqlUpdate(es_table2);


        //将dws层每分钟订单汇总结果 sink到APP层ES
        String insertESSQL2 = "INSERT INTO "+es_table2+
                " SELECT  day_time_str,max(order_num)  FROM "+one_minute_sink_table+
                " group by day_time_str";
        tableEnv.sqlUpdate(insertESSQL2);


        fsEnv.execute(BuyCntPreHour.class.toString());


    }
}

package net.phoenix.bigdata.dw;

import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdOrders2App {

    public static void main(String[] args) throws  Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


        //kafka source
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
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = '"+source_table_name+"',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'latest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")";
        tableEnv.sqlUpdate(dwd_kafka_orders);

        //kafka sink
        String kafka_orders_sink = "dws_kafka_orders";
        String kafkaSourceSQL = "CREATE TABLE " + kafka_orders_sink + "(" +
                "    userId BIGINT," +
                "    province STRING," +
                "    hour_of_day BIGINT," +
                "    order_num BIGINT," +
                "    gmv DOUBLE" +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'update-mode' = 'append',"+
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = '"+kafka_orders_sink+"'," +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"'," +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"'," +
                "    'connector.startup-mode' = 'earliest-offset'," +
                "    'format.type' = 'json'" +
                ")";
        tableEnv.sqlUpdate(kafkaSourceSQL);


        //计算逻辑结果注册临时表
        String resultSql="select " +
                        "   userId,province," +
                        "   HOUR(TUMBLE_START(createTime, INTERVAL '1' HOUR)) as hour_of_day," +
                        "   count(distinct orderId) order_num," +
                        "   sum(realTotalMoney) gmv" +
                        " from  " + source_table_name+
                        " group by userId,province,TUMBLE(createTime, INTERVAL '1' HOUR)";

        Table resultRs = tableEnv.sqlQuery(resultSql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultRs, Row.class);
        tableEnv.createTemporaryView("resultSql",tuple2DataStream);


        //计算结果sink到dws层kafka
        String insertSQL = "INSERT INTO "+kafka_orders_sink+
                " SELECT  userId,province,hour_of_day,order_num,gmv  FROM resultSql";

        tableEnv.sqlUpdate(insertSQL);

        //dws汇总层结果sink到app层es TO
        String es_table ="CREATE TABLE buy_cnt_per_hour ( \n" +
                "    userId BIGINT,\n" +
                "    province STRING,\n" +
                "    hour_of_day BIGINT,\n" +
                "    buy_cnt BIGINT,\n" +
                "    gmv DOUBLE\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch', -- 使用 elasticsearch connector\n" +
                "    'connector.version' = '7',  -- elasticsearch 版本，6 能支持 es 6+ 以及 7+ 的版本\n" +
                "    'connector.hosts' = '"+GlobalConfig.ES_CONNECTOR_URL+"',  -- elasticsearch 地址\n" +
                "    'connector.index' = 'buy_cnt_per_hour',  -- elasticsearch 索引名，相当于数据库的表名\n" +
                "    'connector.document-type' = 'user_behavior', -- elasticsearch 的 type，相当于数据库的库名\n" +
                "    'connector.bulk-flush.max-actions' = '1',  -- 每条数据都刷新\n" +
                "    'format.type' = 'json',  -- 输出数据格式 json\n" +
                "    'update-mode' = 'upsert'\n" +
                ")";
        tableEnv.sqlUpdate(es_table);

        //计算结果sink到dws层kafka
        String insertESSQL = "INSERT INTO buy_cnt_per_hour"+
                " SELECT  userId,province,hour_of_day,max(order_num),max(gmv)  FROM "+kafka_orders_sink+
                " group by userId,province,hour_of_day";
        tableEnv.sqlUpdate(insertESSQL);


        fsEnv.execute(DwdOrders2App.class.toString());


    }
}

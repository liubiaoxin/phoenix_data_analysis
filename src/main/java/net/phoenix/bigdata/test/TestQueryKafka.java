package net.phoenix.bigdata.test;/**
 * Created by QDHL on 2019/10/23.
 */

import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName TestQueryHbase
 * @Description TODO
 * @Author liubiaoxin
 * @Date 2019/10/23 18:24
 * @Version 1.0.0
 **/
public class TestQueryKafka {

    public static void main(String[] args) {

        //获取运行环境
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建一个tableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


        // SQL query with a registered table
        // register a table named "Orders"
        tableEnv.sqlUpdate("CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'user_behavior',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'latest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")");
        // run a SQL query on the Table and retrieve the result as a new Table
        Table result = tableEnv.sqlQuery(
                "SELECT * FROM user_behavior");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(result, Row.class);
        rowDataStream.print();

        try {
            fsEnv.execute(TestQueryKafka.class.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

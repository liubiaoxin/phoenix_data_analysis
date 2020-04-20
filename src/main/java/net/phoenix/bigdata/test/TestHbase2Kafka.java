package net.phoenix.bigdata.test;/**
 * Created by QDHL on 2019/10/23.
 */

import lombok.extern.slf4j.Slf4j;
import net.phoenix.bigdata.common.config.GlobalConfig;
import net.phoenix.bigdata.dws.DwdOrders2Dws;
import org.apache.flink.api.java.tuple.Tuple2;
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
@Slf4j
public class TestHbase2Kafka {

    public static void main(String[] args) throws  Exception{

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        String topic = args[0];
        //hbase source
        String source_table_name = "flink_dwd_orders";
        String hbase_dwd_order_source = "CREATE TABLE " + source_table_name + " (\n" +
                "  rowkey string,\n" +
                "  cf ROW<createTime VARCHAR, goodId VARCHAR, goodsMoney VARCHAR, orderId VARCHAR, orderNo VARCHAR, payFrom VARCHAR, province VARCHAR, realTotalMoney VARCHAR, userId VARCHAR>\n" +
                ") WITH (\n" +
                "  'connector.type' = 'hbase',\n" +
                "  'connector.version' = '1.4.3',\n" +
                "  'connector.table-name' = 'dwd:orders',\n" +
                "  'connector.zookeeper.quorum' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',\n" +
                "  'connector.zookeeper.znode.parent' = '/hbase',\n" +
                "  'connector.write.buffer-flush.max-size' = '100mb',\n" +
                "  'connector.write.buffer-flush.max-rows' = '1000',\n" +
                "  'connector.write.buffer-flush.interval' = '2s'\n" +
                ")";
        tableEnv.sqlUpdate(hbase_dwd_order_source);


        //kafka sink
        String kafkaSourceSQL = "CREATE TABLE dws_kafka_orders(\n" +
                "    user_id VARCHAR,\n" +
                "    order_num BIGINT,\n" +
                "    gmv DOUBLE\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'update-mode' = 'append',"+
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = '"+topic+"',\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    'format.type' = 'json'\n" +
                ")\n";
        tableEnv.sqlUpdate(kafkaSourceSQL);


        //计算逻辑结果注册临时表
        String resultSql="select " +
                "   userId,count(distinct orderId) order_num,sum(CAST(realTotalMoney AS DOUBLE)) gmv" +
                "   from flink_dwd_orders " +
                "   group by userId";

        Table resultRs = tableEnv.sqlQuery(resultSql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultRs, Row.class);
        tableEnv.createTemporaryView("resultSql",tuple2DataStream);


        //插入数据有问题话，需要sink到kafka

        String insertSQL = "INSERT INTO dws_kafka_orders " +
                " SELECT  userId,order_num,gmv\n" +
                "  FROM resultSql";

        tableEnv.sqlUpdate(insertSQL);


        Table table = tableEnv.sqlQuery("SELECT *\n" +
                "FROM (\n" +
                "  SELECT *,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY user_id \n" +
                "      ORDER BY order_num desc,gmv desc) AS rownum\n" +
                "  FROM dws_kafka_orders)\n" +
                "WHERE rownum = 1 ");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream1 = tableEnv.toRetractStream(table, Row.class);
        tuple2DataStream1.print();
        fsEnv.execute(TestHbase2Kafka.class.toString());

    }


}

package net.phoenix.bigdata.test;

import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/* **
 * @Author liubiaoxin
 * @Description //TODO
 * @Date
 * @Param
 * 测试成功
 * #topic创建以及测试
 * bin/kafka-topics.sh --create --zookeeper 192.168.1.103:2181  --replication-factor 2 --partitions 1 --topic user_log
 * bin/kafka-console-producer.sh --broker-list 192.168.1.103:9092 --topic user_log
 * {"user_id": "12331", "item_id":"310884", "category_id": "4580532", "behavior": "pv", "ts": "2017-11-27T00:00:00Z"}
 * bin/kafka-console-consumer.sh --zookeeper 192.168.1.103:2181  --topic user_log
 *
 * #hbase建表
 * create 'hbase_test','CF'
 * scan 'hbase_test'
 **/
public class TestKafka2Hbase {

    public static void main(String[] args) throws Exception{
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance().inStreamingMode()
                .useBlinkPlanner().build();

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //streamEnv.enableCheckpointing(taskConfig.getCheckpointInterval() * 1000L);
        //streamEnv.getConfig().setLatencyTrackingInterval(LATENCY_TRACK_INTERVAL);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, streamSettings);

        String topic = args[0];

        String hbase_table = args[1];

        //kafka源sql
        String kafkaSourceSQL = "CREATE TABLE user_log(\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = '"+topic+"',\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    'format.type' = 'json'\n" +
                ")\n";

        String hbaseSinkSQL = "CREATE TABLE user_log_sink (\n" +
                "    rowkey string,\n" +
                "    CF ROW(item_id VARCHAR, category_id VARCHAR, behavior VARCHAR, ts TIMESTAMP(3),cnt bigint)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'hbase',\n" +
                "    'connector.version' = '1.4.3',                    -- hbase vesion\n" +
                "    'connector.table-name' = '" + hbase_table + "',                  -- hbase table name\n" +
                "    'connector.zookeeper.quorum' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',       -- zookeeper quorum\n" +
                "    'connector.zookeeper.znode.parent' = '/hbase',    -- hbase znode in zookeeper\n" +
                "    'connector.write.buffer-flush.max-size' = '10mb', -- max flush size\n" +
                "    'connector.write.buffer-flush.max-rows' = '1000', -- max flush rows\n" +
                "    'connector.write.buffer-flush.interval' = '2s'    -- max flush interval\n" +
                ")";


        streamTableEnv.sqlUpdate(kafkaSourceSQL);
        streamTableEnv.sqlUpdate(hbaseSinkSQL);

        String reslultSql = " select " +
                            "       item_id, category_id, behavior, ts,count(*) cnt " +
                            "       from user_log " +
                            "       group by item_id, category_id, behavior, ts";
        Table countRs = streamTableEnv.sqlQuery(reslultSql);

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = streamTableEnv.toRetractStream(countRs, Row.class);

        streamTableEnv.createTemporaryView("countRS",tuple2DataStream);

        String insertSQL = "  insert into user_log_sink  " +
                "   select item_id,ROW(item_id, category_id, behavior, ts, cnt) AS CF from  countRS";


        streamTableEnv.sqlUpdate(insertSQL);

        streamEnv.execute(TestKafka2Hbase.class.toString());
    }
}

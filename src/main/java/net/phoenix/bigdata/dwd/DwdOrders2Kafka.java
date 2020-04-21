package net.phoenix.bigdata.dwd;

import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdOrders2Kafka {

    public static void main(String[] args) throws  Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);


        //Hbase source
        String source_hbase_table = "dwd_orders_hbase_src";
        String hbase_table_src = "CREATE TABLE " + source_hbase_table + " (\n" +
                "  rowkey string,\n" +
                "  cf ROW<createTime VARCHAR, goodId VARCHAR, goodsMoney VARCHAR, orderId VARCHAR, orderNo VARCHAR, payFrom VARCHAR, province VARCHAR, realTotalMoney VARCHAR, userId VARCHAR >\n" +
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
        tableEnv.sqlUpdate(hbase_table_src);

        //kafka sink
        String sink_table_name = "dwd_kafka_orders";
        String dwd_kafka_orders = "CREATE TABLE "+sink_table_name+" (\n" +
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
                "    'connector.topic' = '"+sink_table_name+"',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'latest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")";
        tableEnv.sqlUpdate(dwd_kafka_orders);


        //插入数据有问题话，需要sink到kafka
        String insertSQL = "INSERT INTO "+sink_table_name+
                " SELECT  CAST(orderId as BIGINT) orderId, " +
                "  orderNo, " +
                " CAST(userId as BIGINT) userId, " +
                " CAST(goodId as BIGINT) goodId, " +
                " CAST(goodsMoney as DOUBLE) goodsMoney," +
                " CAST(realTotalMoney as DOUBLE) realTotalMoney," +
                " CAST(payFrom as BIGINT) payFrom, " +
                " province, " +
                " CAST(createTime as TIMESTAMP) createTime" +
                " FROM "+source_hbase_table+
                " where  substring(createTime,1,13)=substring(DATE_FORMAT(TIMESTAMPADD(HOUR, 8, CURRENT_TIMESTAMP),'yyyy-MM-dd HH:mm:ss'),1,13)";

        tableEnv.sqlUpdate(insertSQL);
        fsEnv.execute(DwdOrders2Kafka.class.toString());


    }
}

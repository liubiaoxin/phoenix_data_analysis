package net.phoenix.bigdata.dws;

import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdOrders2Dws {

    public static void main(String[] args) throws  Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        // SQL query with a registered table
        // register a table named "Orders"
        String source_table_name = "flink_dwd_orders";
        String hbase_dwd_order_source = "CREATE TABLE " + source_table_name + " (\n" +
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
        tableEnv.sqlUpdate(hbase_dwd_order_source);



        //hbase sink
        String sink_table_name = "flink_dws_orders";
        String hbase_dws_order_sink = "CREATE TABLE " + sink_table_name + " (\n" +
                "  rowkey string,\n" +
                "  cf ROW<user_id VARCHAR,order_num BIGINT,gmv DOUBLE >\n" +
                ") WITH (\n" +
                "  'connector.type' = 'hbase',\n" +
                "  'connector.version' = '1.4.3',\n" +
                "  'connector.table-name' = 'dws:orders',\n" +
                "  'connector.zookeeper.quorum' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',\n" +
                "  'connector.zookeeper.znode.parent' = '/hbase',\n" +
                "  'connector.write.buffer-flush.max-size' = '100mb',\n" +
                "  'connector.write.buffer-flush.max-rows' = '1000',\n" +
                "  'connector.write.buffer-flush.interval' = '2s'\n" +
                ")";
        tableEnv.sqlUpdate(hbase_dws_order_sink);

        //计算逻辑结果注册临时表
        String resultSql="select " +
                "   userId,count(distinct orderId) order_num,sum(CAST(realTotalMoney AS DOUBLE)) gmv" +
                "   from flink_dwd_orders " +
                "   group by userId";

        Table resultRs = tableEnv.sqlQuery(resultSql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultRs, Row.class);
        tableEnv.createTemporaryView("resultSql",tuple2DataStream);


        //插入数据有问题话，需要sink到kafka

        String insertSQL = "INSERT INTO flink_dws_orders " +
                " SELECT  userId,ROW(userId,order_num,gmv)\n" +
                "  FROM resultSql";

        tableEnv.sqlUpdate(insertSQL);

        Table table = tableEnv.sqlQuery("select * from  flink_dws_orders");
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        rowDataStream.print();

        fsEnv.execute(DwdOrders2Dws.class.toString());


    }
}

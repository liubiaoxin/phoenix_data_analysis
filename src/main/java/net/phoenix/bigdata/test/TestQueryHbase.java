package net.phoenix.bigdata.test;/**
 * Created by QDHL on 2019/10/23.
 */

import lombok.extern.slf4j.Slf4j;
import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.addons.hbase.HBaseUpsertSinkFunction;
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
public class TestQueryHbase {

    public static void main(String[] args) throws Exception{


        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        String  sql = args[0];
        log.info("--------------------------"+sql);
        String table_name = "flink_dwd_orders";

        //已测试通过
        /*String query_sql = " CREATE TABLE "+table_name+" (\n" +
        "    rowkey string,\n" +
                "    CF ROW(item_id VARCHAR, category_id VARCHAR, behavior VARCHAR, ts TIMESTAMP(3))\n" +
                ") WITH (\n" +
                "    'connector.type' = 'hbase',\n" +
                "    -- 目前只支持 1.4.3 ，   HBaseValidator.CONNECTOR_VERSION_VALUE_143 写死了 1.4.3, 改成 2.1.4 可以正常写数到 hbase\n" +
                "    -- 生产慎用\n" +
                "    'connector.version' = '1.4.3',                    -- hbase vesion\n" +
                "    'connector.table-name' = 'hbase_test',                  -- hbase table name\n" +
                "    'connector.zookeeper.quorum' = '192.168.5.80:2181',       -- zookeeper quorum\n" +
                "    'connector.zookeeper.znode.parent' = '/hbase',    -- hbase znode in zookeeper\n" +
                "    'connector.write.buffer-flush.max-size' = '10mb', -- max flush size\n" +
                "    'connector.write.buffer-flush.max-rows' = '1000', -- max flush rows\n" +
                "    'connector.write.buffer-flush.interval' = '2s'    -- max flush interval\n" +
                ")";*/

        String query_sql = "CREATE TABLE " + table_name + " (\n" +
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


        tableEnv.sqlUpdate(query_sql);
        Table result = tableEnv.sqlQuery(sql);

        result.printSchema();
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(result,Row.class);
        tuple2DataStream.print();
        try {
            fsEnv.execute(TestQueryHbase.class.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}

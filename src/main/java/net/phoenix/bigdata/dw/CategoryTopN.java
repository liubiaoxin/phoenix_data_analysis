package net.phoenix.bigdata.dw;

import net.phoenix.bigdata.common.config.GlobalConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class CategoryTopN {

    public static void main(String[] args) throws  Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        //设置检查点
        fsEnv.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取ODS层kafka topic为ods_log_origin注册为日志用户行为表dwd_user_behavior
        String source_table_name = "dwd_user_behavior";
        String source_table_sql = "CREATE TABLE "+source_table_name+" (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    proctime as PROCTIME(),   -- 通过计算列产生一个处理时间列\n" +
                "    WATERMARK FOR ts as ts - INTERVAL '5' SECOND  -- 在ts上定义watermark，ts成为事件时间列\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',  -- 使用 kafka connector\n" +
                "    'connector.properties.group.id' = 'ods_log_origin_group2',"+
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'ods_log_origin',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")";
        tableEnv.sqlUpdate(source_table_sql);

        //mysql维表注册

        String category_table_name="dim_category";
        String category_table_sql="CREATE TABLE "+category_table_name+" (\n" +
                "    sub_category_id INT,  -- 子类目\n" +
                "    parent_category_id INT, -- 顶级类目\n" +
                "    parent_category_name STRING -- 顶级类目名称\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://bigdata-01:3306/test',\n" +
                "    'connector.table' = 'category',\n" +
                "    'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'connector.username' = 'test',\n" +
                "    'connector.password' = '123456',\n" +
                "    'connector.lookup.cache.max-rows' = '5000',\n" +
                "    'connector.lookup.cache.ttl' = '10min'\n" +
                ")";
        tableEnv.sqlUpdate(category_table_sql);



        //ES结果表注册
        String es_rs_table="top_category";
        String es_table_sql="CREATE TABLE "+es_rs_table+" (\n" +
                "    category_name STRING,  -- 类目名称\n" +
                "    buy_cnt BIGINT  -- 销量\n" +
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
        tableEnv.sqlUpdate(es_table_sql);

        //join mysql维表，丰富dwd_user_behavior表维度
        String rich_user_behavior_sql=" SELECT U.user_id, U.item_id, U.behavior, \n" +
                "  CASE WHEN C.parent_category_id in (1,2,3,4,5,6,7,8) THEN C.parent_category_name\n" +
                "    ELSE '其他'\n" +
                "  END AS category_name\n" +
                "FROM "+source_table_name+" AS U LEFT JOIN "+category_table_name+" FOR SYSTEM_TIME AS OF U.proctime AS C\n" +
                "ON U.category_id = C.sub_category_id";

        Table table = tableEnv.sqlQuery(rich_user_behavior_sql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);
        tableEnv.createTemporaryView("dwd_rich_user_behavior",tuple2DataStream);


        //最总结果sink到ES
        String insertEsSql="INSERT INTO "+es_rs_table+"\n" +
                "SELECT category_name, COUNT(*) buy_cnt\n" +
                "FROM dwd_rich_user_behavior\n" +
                "WHERE behavior = 'buy'\n" +
                "GROUP BY category_name";

        tableEnv.sqlUpdate(insertEsSql);


        fsEnv.execute(CategoryTopN.class.toString());


    }
}

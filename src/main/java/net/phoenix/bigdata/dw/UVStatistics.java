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

public class UVStatistics {

    public static void main(String[] args) throws  Exception{
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        //设置检查点
        fsEnv.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取ODS层kafka topic为ods_log_origino注册为日志用户行为表dwd_user_behavior
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
                "    'connector.properties.group.id' = 'ods_log_origin_group1',"+
                "    'connector.version' = 'universal',  -- kafka 版本，universal 支持 0.11 以上的版本\n" +
                "    'connector.topic' = 'ods_log_origin',  -- kafka topic\n" +
                "    'connector.startup-mode' = 'earliest-offset',  -- 从起始 offset 开始读取\n" +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"',  -- zookeeper 地址\n" +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"',  -- kafka broker 地址\n" +
                "    'format.type' = 'json'  -- 数据源格式为 json\n" +
                ")";
        tableEnv.sqlUpdate(source_table_sql);


        //dws层dws_per_10min_uv表：统计10分钟累计uv
        String kafka_sink_table = "dws_per_10min_uv";
        String kafkaSinkSql = "CREATE TABLE " + kafka_sink_table + "(" +
                "    day_time_str STRING," +
                "    uv BIGINT" +
                ") WITH (" +
                "    'connector.type' = 'kafka'," +
                "    'update-mode' = 'append',"+
                "    'connector.properties.group.id' = '"+kafka_sink_table+"_group1',"+
                "    'connector.version' = 'universal'," +
                "    'connector.topic' = '"+ kafka_sink_table +"'," +
                "    'connector.properties.zookeeper.connect' = '"+GlobalConfig.KAFKA_ZK_CONNECTS+"'," +
                "    'connector.properties.bootstrap.servers' = '"+GlobalConfig.KAFKA_SERVERS+"'," +
                "    'connector.startup-mode' = 'earliest-offset'," +
                "    'format.type' = 'json'" +
                ")";
        tableEnv.sqlUpdate(kafkaSinkSql);


        //1分钟统计订单逻辑
        String per_10min_uv_sql="select day_time_str,uv from ( select max(substring(DATE_FORMAT(ts,'yyyy-MM-dd HH:mm:ss'),1,15)||'0') OVER w AS day_time_str,"+
                "   count(distinct user_id) OVER w AS uv" +
                " from  " + source_table_name+
                " WINDOW w AS(ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ) temp";
        //注册成临时表
        Table table = tableEnv.sqlQuery(per_10min_uv_sql);
        table.printSchema();
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream1 = tableEnv.toRetractStream(table, Row.class);
        tableEnv.createTemporaryView("view_per_10min_uv",tuple2DataStream1);

        tuple2DataStream1.print();

       //每分钟订单汇总结果sink到dws层kafka
        String insert_per_minute_SQL = "INSERT INTO "+ kafka_sink_table +
                " SELECT  day_time_str,uv FROM view_per_10min_uv";

        tableEnv.sqlUpdate(insert_per_minute_SQL);


         /*
        //注册APP层ES结果表
        String es_rs_table = "per_10min_uv";
        String es_table = "CREATE TABLE " + es_rs_table + " ( \n" +
                "    day_time_str STRING,\n" +
                "    uv BIGINT\n" +
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
                " SELECT  day_time_str,max(uv)  FROM "+kafka_sink_table+
                " group by day_time_str";
        tableEnv.sqlUpdate(insertESSQL);*/


        fsEnv.execute(UVStatistics.class.toString());


    }
}

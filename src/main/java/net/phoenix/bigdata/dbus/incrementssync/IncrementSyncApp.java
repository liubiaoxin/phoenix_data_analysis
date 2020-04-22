package net.phoenix.bigdata.dbus.incrementssync;

import com.alibaba.otter.canal.protocol.FlatMessage;
import net.phoenix.bigdata.common.config.GlobalConfig;
import net.phoenix.bigdata.dbus.function.DbusProcessFunction;
import net.phoenix.bigdata.dbus.sink.HbaseSyncSink;
import net.phoenix.bigdata.dbus.source.FlowSoure;
import net.phoenix.bigdata.pojo.FlatMessageSchema;
import net.phoenix.bigdata.pojo.Flow;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 实时增量同步模块
 *
 * @author liubiaoxin
 *
 */
public class IncrementSyncApp {

    public static final MapStateDescriptor<String, Flow> flowStateDescriptor =
            new MapStateDescriptor<String, Flow>("flowBroadCastState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<Flow>() {
            }));
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStateBackend(new RocksDBStateBackend("hdfs://bigdata-02:9000/flink/flink-checkpoints/"));
        //设置检查点
        sEnv.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.put("bootstrap.servers", GlobalConfig.KAFKA_SERVERS);
        props.put("zookeeper.connect", GlobalConfig.KAFKA_ZK_CONNECTS);
        props.put("group.id", GlobalConfig.KAFKA_GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        props.put("flink.partition-discovery.interval-millis","30000");

        //消费kafka数据
        FlinkKafkaConsumer<FlatMessage> myConsumer = new FlinkKafkaConsumer<FlatMessage>("test", new FlatMessageSchema(), props);
        DataStream<FlatMessage> message = sEnv.addSource(myConsumer);


        //message.print();
        //同库，同表数据进入同一个分组，一个分区
        KeyedStream<FlatMessage, String> keyedMessage = message.keyBy(new KeySelector<FlatMessage, String>() {
            @Override
            public String getKey(FlatMessage value) throws Exception {
                return value.getDatabase() + value.getTable();
            }
        });
        //keyedMessage.print();

        //读取配置流
        BroadcastStream<Flow> broadcast = sEnv.addSource(new FlowSoure()).setParallelism(1).broadcast(flowStateDescriptor);


        //连接数据流和配置流
        DataStream<Tuple2<FlatMessage,Flow>> connectedStream = keyedMessage.connect(broadcast)
                .process(new DbusProcessFunction())
                .setParallelism(1);

        //connectedStream.print();

        connectedStream.addSink(new HbaseSyncSink()).setParallelism(1);

        sEnv.execute("IncrementSyncApp");
    }
}

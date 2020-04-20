package net.phoenix.bigdata.dbus.sink;

import com.alibaba.otter.canal.protocol.FlatMessage;
import net.phoenix.bigdata.common.config.GlobalConfig;
import net.phoenix.bigdata.pojo.Flow;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseSyncSink extends RichSinkFunction<Tuple2<FlatMessage,Flow>> {

    private HbaseSyncService hbaseSyncService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        org.apache.hadoop.conf.Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", GlobalConfig.HBASE_ZK_HOST);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", GlobalConfig.HBASE_ZK_PORT);
        hbaseConfig.set("zookeeper.znode.parent", GlobalConfig.HBASE_ZK_PATH);

        HbaseTemplate hbaseTemplate = new HbaseTemplate(hbaseConfig);
        hbaseSyncService = new HbaseSyncService(hbaseTemplate);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(Tuple2<FlatMessage, Flow> value, Context context) throws Exception {
        hbaseSyncService.sync(value.f1, value.f0);
    }

}

package net.phoenix.bigdata.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.model.*;

import java.util.ArrayList;
import java.util.List;

public class PutData2Topic {

    public static void main(String[] args) {

        String DATAHUB_ENDPOINT="https://dh-cn-beijing.aliyuncs.com";
        String ACESS_ID="LTAI4G1okrTNR1FZx82KuGzv";
        String ACCESS_KEY="Z5qgsNf1X29l4huN4B0rlmF94RSmmx";

        DatahubClient client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(DATAHUB_ENDPOINT, new AliyunAccount(ACESS_ID, ACCESS_KEY))
                ).build();


        String PROJECT_NAME="px_realtime_dw";
        String TOPIC_NAME="test";

        RecordSchema schema = client.getTopic(PROJECT_NAME, TOPIC_NAME).getRecordSchema();


        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int cnt = 0; cnt < 10; ++cnt) {
            RecordEntry entry = new RecordEntry();
            entry.addAttribute("key1", "value1");
            entry.addAttribute("key2", "value2");

            TupleRecordData data = new TupleRecordData(schema);
            data.setField("field1", cnt);
            data.setField("field2", "filed2"+cnt);

            entry.setRecordData(data);
            recordEntries.add(entry);
        }

        // put tuple records by shard
        //client.putRecordsByShard(PROJECT_NAME, TOPIC_NAME, SHARD_ID, recordEntries);
        client.putRecords(PROJECT_NAME,TOPIC_NAME,recordEntries);
    }
}

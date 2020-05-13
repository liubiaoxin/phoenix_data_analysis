package net.phoenix.bigdata.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.model.*;
import org.apache.avro.generic.GenericData;

import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.List;

public class ConsumerTopic {
    public static void main(String[] args) {
        String DATAHUB_ENDPOINT="https://dh-cn-beijing.aliyuncs.com";
        String ACESS_ID="LTAI4G1okrTNR1FZx82KuGzv";
        String ACCESS_KEY="Z5qgsNf1X29l4huN4B0rlmF94RSmmx";

        DatahubClient client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(DATAHUB_ENDPOINT, new AliyunAccount(ACESS_ID, ACCESS_KEY))
                ).build();


        String PROJECT_NAME="px_realtime_dw";
        String TOPIC_NAME="order_detail";
        String SHARD_ID="0";
        RecordSchema schema = client.getTopic(PROJECT_NAME, TOPIC_NAME).getRecordSchema();


        List<String> field_str= new ArrayList<>();
        List<Field> fields = schema.getFields();
        StringBuffer title=new StringBuffer();
        for (Field field:fields
             ) {
            field_str.add(field.getName());
            title.append(field.getName()+",");
        }
        // get records
        GetCursorResult getCursorResult = client.getCursor(PROJECT_NAME, TOPIC_NAME, SHARD_ID, CursorType.SEQUENCE, 0);
        GetRecordsResult getRecordsResult = client.getRecords(PROJECT_NAME, TOPIC_NAME, SHARD_ID, schema, getCursorResult.getCursor(), 100);
        System.out.println(title.toString());
        for (RecordEntry entry : getRecordsResult.getRecords()) {
            TupleRecordData data = (TupleRecordData) entry.getRecordData();
            StringBuffer out = new StringBuffer();
            for (String field: field_str
                 ) {
                out.append(data.getField(field)).append(",");

            }
            System.out.println(out.toString());
        }




    }
}

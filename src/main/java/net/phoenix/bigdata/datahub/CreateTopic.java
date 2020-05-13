package net.phoenix.bigdata.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;

public class CreateTopic {

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
        int shardCount = 3;
        int lifecycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.BIGINT));
        schema.addField(new Field("field2", FieldType.STRING));
        String comment = "The first topic";
        client.createTopic(PROJECT_NAME, TOPIC_NAME, shardCount, lifecycle, type, schema, comment);




    }
}

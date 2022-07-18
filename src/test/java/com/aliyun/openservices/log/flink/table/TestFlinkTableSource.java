package com.aliyun.openservices.log.flink.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

public class TestFlinkTableSource {
    private static final String SLS_ENDPOINT = "your_endpoint";
    private static final String SLS_ACCESS_KEY_ID = "your_access_key_id";
    private static final String SLS_ACCESS_KEY_SECRET = "your_access_key_secret";
    private static final String SLS_PROJECT = "your_project";
    private static final String SLS_LOGSTORE = "your_logstore";

    @Test
    public void testFlinkTableSource() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build());
        env.executeSql("CREATE TABLE sls_test_table (\n" +
                "  id STRING,\n" +
                "  name STRING,\n" +
                "  age BIGINT,\n" +
                "  desc STRING,\n" +
                "  ts TIMESTAMP_LTZ\n" +
                ") WITH (\n" +
                "   'connector' = 'aliyun-sls',\n" +
                "   'ENDPOINT' = '" + SLS_ENDPOINT + "',\n" +
                "   'ACCESSKEY' = '" + SLS_ACCESS_KEY_SECRET + "',\n" +
                "   'ACCESSKEYID' = '" + SLS_ACCESS_KEY_ID + "',\n" +
                "   'PROJECT' = '" + SLS_PROJECT + "',\n" +
                "   'LOGSTORE' = '" + SLS_LOGSTORE + "',\n" +
                "   'CONSUMER_GROUP' = 'local_test'\n" +
                ")");
        env.sqlQuery("select * from sls_test_table limit 10")
                .execute()
                .print();
    }
}

package com.aliyun.openservices.log.flink.table;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.model.CheckpointMode;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class TableOptions {
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_ENDPOINT = ConfigOptions
            .key(ConfigConstants.LOG_ENDPOINT)
            .stringType()
            .noDefaultValue()
            .withDescription("Required Aliyun-SLS endpoint connection string");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_ACCESS_KEY = ConfigOptions
            .key(ConfigConstants.LOG_ACCESSKEY)
            .stringType()
            .noDefaultValue()
            .withDescription("Required Aliyun-SLS accessKey connection string");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_ACCESS_KEY_ID = ConfigOptions
            .key(ConfigConstants.LOG_ACCESSKEYID)
            .stringType()
            .noDefaultValue()
            .withDescription("Required Aliyun-SLS accessKeyId connection string");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_PROJECT = ConfigOptions
            .key(ConfigConstants.LOG_PROJECT)
            .stringType()
            .noDefaultValue()
            .withDescription("Required Aliyun-SLS project connection string");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_LOGSTORE = ConfigOptions
            .key(ConfigConstants.LOG_LOGSTORE)
            .stringType()
            .noDefaultValue()
            .withDescription("Required Aliyun-SLS logstore connection string");

    public static final ConfigOption<String> PROPS_ALIYUN_SLS_CONSUMER_GROUP = ConfigOptions
            .key(ConfigConstants.LOG_CONSUMERGROUP)
            .stringType()
            .defaultValue("flink-connector-group")
            .withDescription("Aliyun-SLS consumer group string");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_CONSUMER_BEGIN_POSITION = ConfigOptions
            .key(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION)
            .stringType()
            .defaultValue(Consts.LOG_FROM_CHECKPOINT)
            .withDescription("Aliyun-SLS consumer begin position. optional [begin_cursor, end_cursor, consumer_from_checkpoint]");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_MAX_NUMBER_PER_FETCH = ConfigOptions
            .key(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH)
            .stringType()
            .defaultValue(String.valueOf(Consts.DEFAULT_NUMBER_PER_FETCH))
            .withDescription("Aliyun-SLS consumer fetch max number");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_CHECKPOINT_MODE = ConfigOptions
            .key(ConfigConstants.LOG_CHECKPOINT_MODE)
            .stringType()
            .defaultValue(CheckpointMode.ON_CHECKPOINTS.name())
            .withDescription("Aliyun-SLS flink consumer checkpoint mode. optional [DISABLED, ON_CHECKPOINTS, PERIODIC]");
    public static final ConfigOption<Long> PROPS_ALIYUN_SLS_STOP_TIME = ConfigOptions
            .key(ConfigConstants.STOP_TIME)
            .longType()
            .noDefaultValue()
            .withDescription("Aliyun-SLS flink consumer begin time");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_COMMIT_INTERVAL_MILLIS = ConfigOptions
            .key(ConfigConstants.LOG_COMMIT_INTERVAL_MILLIS)
            .stringType()
            .defaultValue(String.valueOf(Consts.DEFAULT_COMMIT_INTERVAL_MILLIS))
            .withDescription("Aliyun-SLS consumer commit interval millis");
    public static final ConfigOption<String> PROPS_ALIYUN_SLS_LOG_DESERIALIZER = ConfigOptions
            .key("log.deserializer")
            .stringType()
            .defaultValue(DefaultRowDataDeserializationSchema.class.getName())
            .withDescription("Aliyun-SLS log deserializer.");
}

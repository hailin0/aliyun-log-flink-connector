package com.aliyun.openservices.log.flink.model;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

public interface LogDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    /**
     * Deserialize pull logs result to Flink records.
     *
     * @param record LogGroup list.
     * @param collector log item collector
     */
    void deserialize(PullLogsResult record, Collector<T> collector);
}

package com.aliyun.openservices.log.flink.model;

public interface Collector<T> {
    default void collect(T record) {
        collectWithTimestamp(record, null);
    }

    void collectWithTimestamp(T record, Long timestamp);
}
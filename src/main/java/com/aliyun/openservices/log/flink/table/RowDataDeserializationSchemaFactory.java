package com.aliyun.openservices.log.flink.table;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

public class RowDataDeserializationSchemaFactory {

    private String logDeserializer;

    public RowDataDeserializationSchemaFactory(Map<String, String> tableOptions) {
        this.logDeserializer = tableOptions.getOrDefault(TableOptions.PROPS_ALIYUN_SLS_LOG_DESERIALIZER.key(),
            TableOptions.PROPS_ALIYUN_SLS_LOG_DESERIALIZER.defaultValue());
    }

    public RowDataDeserializationSchema create(TypeInformation<RowData> typeInformation, RowType rowType) {
        try {
            Class<RowDataDeserializationSchema> clazz =
                (Class<RowDataDeserializationSchema>) ClassUtils.getClass(logDeserializer);
            return ConstructorUtils.invokeConstructor(clazz, typeInformation, rowType);
        } catch (Exception e) {
            throw new IllegalArgumentException("log deserializer invalid", e);
        }
    }
}

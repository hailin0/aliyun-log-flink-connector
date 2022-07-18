package com.aliyun.openservices.log.flink.table;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.Collector;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultRowDataDeserializationSchema extends RowDataDeserializationSchema {
    private RowType rowType;

    public DefaultRowDataDeserializationSchema(TypeInformation<RowData> typeInformation, RowType rowType) {
        super(typeInformation, rowType);
        this.rowType = rowType;
    }

    @Override
    public void deserialize(PullLogsResult record, Collector<RowData> collector) {
        List<LogGroupData> records = record.getLogGroupList();
        if (records == null || records.isEmpty()) {
            return;
        }

        for (LogGroupData logGroupData : records) {
            FastLogGroup logGroup = logGroupData.GetFastLogGroup();
            if (logGroup == null || logGroup.getLogsCount() == 0) {
                continue;
            }

            for (int i = 0; i < logGroup.getLogsCount(); i++) {
                FastLog log = logGroup.getLogs(i);
                if (log.getContentsCount() > 0) {
                    RowData rowData = toRowData(log, rowType);
                    if (rowData != null) {
                        long logTimestamp = log.getTime() * 1000;
                        collector.collectWithTimestamp(rowData, logTimestamp);
                    }
                }
            }
        }
    }

    private RowData toRowData(FastLog log, RowType rowType) {
        Map<String, String> eventMap = toEventMap(log);
        if (eventMap.isEmpty()) {
            return null;
        }

        List<RowField> rowFields = rowType.getFields();
        GenericRowData rowData = new GenericRowData(rowFields.size());
        List<String> matchFields = new ArrayList<>();
        for (int i = 0; i < rowFields.size(); i++) {
            RowField rowField = rowFields.get(i);
            String fieldValue = eventMap.get(rowField.getName());
            if (!rowField.getType().isNullable() && fieldValue == null) {
                return null;
            }

            Object rowFieldData = null;
            if (fieldValue != null) {
                rowFieldData = toFlinkType(rowField, fieldValue);
            }

            matchFields.add(rowField.getName());
            rowData.setField(i, rowFieldData);
        }
        return matchFields.isEmpty() ? null : rowData;
    }

    protected Map<String, String> toEventMap(FastLog log) {
        Map<String, String> logContentMap = new HashMap<>();

        for (int i = 0; i < log.getContentsCount(); i++) {
            FastLogContent logContent = log.getContents(i);
            String key = logContent.getKey();
            String value = logContent.getValue();
            logContentMap.put(key, value);
        }
        return logContentMap;
    }

    private static Object toFlinkType(RowField rowField, String fieldValue) {
        LogicalType logicalType = rowField.getType();
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
                return Boolean.valueOf(fieldValue);
            case TINYINT:
                return Byte.valueOf(fieldValue);
            case SMALLINT:
                return Short.valueOf(fieldValue);
            case INTEGER:
                return Integer.valueOf(fieldValue);
            case BIGINT:
                return Long.valueOf(fieldValue);
            case FLOAT:
                return Float.valueOf(fieldValue);
            case DOUBLE:
                return Double.valueOf(fieldValue);
            case DECIMAL:
                int precision = ((DecimalType) logicalType).getPrecision();
                int scale = ((DecimalType) logicalType).getScale();
                BigDecimal val = new BigDecimal(fieldValue);
                return DecimalData.fromBigDecimal(val, precision, scale);
            case CHAR:
            case VARCHAR:
                return new BinaryStringData(fieldValue);
            case DATE:
                return (int) LocalDate.parse(fieldValue, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return (int) LocalTime.parse(fieldValue, DateTimeFormatter.ISO_LOCAL_TIME).toNanoOfDay() / 1_000_000L;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromEpochMillis(Long.valueOf(fieldValue));
            default:
                throw new UnsupportedOperationException("Unsupported dataType: " + typeRoot.name());
        }
    }
}

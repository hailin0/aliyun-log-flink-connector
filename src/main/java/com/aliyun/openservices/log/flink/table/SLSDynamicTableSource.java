package com.aliyun.openservices.log.flink.table;

import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Properties;

public class SLSDynamicTableSource implements ScanTableSource {

    private Properties properties;
    private DataType physicalDataType;
    private RowDataDeserializationSchemaFactory rowDataDeserializationSchemaFactory;

    public SLSDynamicTableSource(Properties properties,
                                 DataType physicalDataType,
                                 RowDataDeserializationSchemaFactory rowDataDeserializationSchemaFactory) {
        this.properties = properties;
        this.physicalDataType = physicalDataType;
        this.rowDataDeserializationSchemaFactory = rowDataDeserializationSchemaFactory;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(physicalDataType);
        RowType rowType = (RowType)physicalDataType.getLogicalType();
        LogDeserializationSchema<RowData> logDeserializationSchema = rowDataDeserializationSchemaFactory.create(
                producedTypeInfo, rowType);

        FlinkLogConsumer<RowData> flinkLogConsumer = new FlinkLogConsumer<>(logDeserializationSchema, properties);
        return SourceFunctionProvider.of(flinkLogConsumer, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SLSDynamicTableSource(properties, physicalDataType, rowDataDeserializationSchemaFactory);
    }

    @Override
    public String asSummaryString() {
        return "Aliyun-SLS table source";
    }
}

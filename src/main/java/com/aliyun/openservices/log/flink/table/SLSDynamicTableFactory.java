package com.aliyun.openservices.log.flink.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Internal
public class SLSDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    private static final String IDENTIFIER = "aliyun-sls";
    private static final Collection<String> EXCLUDE_TABLE_OPTIONS = Arrays.asList("connector", "log.deserializer");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TableOptions.PROPS_ALIYUN_SLS_ENDPOINT);
        options.add(TableOptions.PROPS_ALIYUN_SLS_ACCESS_KEY);
        options.add(TableOptions.PROPS_ALIYUN_SLS_ACCESS_KEY_ID);
        options.add(TableOptions.PROPS_ALIYUN_SLS_PROJECT);
        options.add(TableOptions.PROPS_ALIYUN_SLS_LOGSTORE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TableOptions.PROPS_ALIYUN_SLS_CONSUMER_GROUP);
        options.add(TableOptions.PROPS_ALIYUN_SLS_CONSUMER_BEGIN_POSITION);
        options.add(TableOptions.PROPS_ALIYUN_SLS_MAX_NUMBER_PER_FETCH);
        options.add(TableOptions.PROPS_ALIYUN_SLS_CHECKPOINT_MODE);
        options.add(TableOptions.PROPS_ALIYUN_SLS_COMMIT_INTERVAL_MILLIS);
        options.add(TableOptions.PROPS_ALIYUN_SLS_STOP_TIME);
        options.add(TableOptions.PROPS_ALIYUN_SLS_LOG_DESERIALIZER);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        Properties slsProperties = getSLSProperties(tableOptions);
        DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        RowDataDeserializationSchemaFactory rowDataDeserializationSchemaFactory =
            new RowDataDeserializationSchemaFactory(tableOptions);

        return new SLSDynamicTableSource(slsProperties,
                physicalDataType, rowDataDeserializationSchemaFactory);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        throw new UnsupportedOperationException();
    }

    private static Properties getSLSProperties(Map<String, String> tableOptions) {
        Properties slsProperties = new Properties();

        tableOptions.entrySet()
                .stream()
                .filter(e -> !EXCLUDE_TABLE_OPTIONS.contains(e.getKey().toLowerCase()))
                .forEach(e -> slsProperties.put(e.getKey(), e.getValue()));
        return slsProperties;
    }
}

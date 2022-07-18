package com.aliyun.openservices.log.flink.data;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.flink.model.Collector;
import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import com.aliyun.openservices.log.flink.model.PullLogsResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.util.List;

public class FastLogGroupDeserializer implements LogDeserializationSchema<FastLogGroupList> {

    @Override
    public void deserialize(PullLogsResult record, Collector<FastLogGroupList> collector) {
        List<LogGroupData> logGroupDataList = record.getLogGroupList();
        int count = logGroupDataList == null ? 0 : logGroupDataList.size();
        FastLogGroupList logGroupList = new FastLogGroupList(count);
        if (logGroupDataList != null && !logGroupDataList.isEmpty()) {
            for (LogGroupData logGroupData : logGroupDataList) {
                logGroupList.addLogGroup(logGroupData.GetFastLogGroup());
            }
        }
        collector.collect(logGroupList);
    }

    @Override
    public TypeInformation<FastLogGroupList> getProducedType() {
        return PojoTypeInfo.of(FastLogGroupList.class);
    }
}

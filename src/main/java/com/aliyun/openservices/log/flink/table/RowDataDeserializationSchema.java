package com.aliyun.openservices.log.flink.table;

import com.aliyun.openservices.log.flink.model.LogDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public abstract class RowDataDeserializationSchema implements LogDeserializationSchema<RowData> {

  private TypeInformation<RowData> typeInformation;
  private RowType rowType;

  public RowDataDeserializationSchema(TypeInformation<RowData> typeInformation, RowType rowType) {
    this.typeInformation = typeInformation;
    this.rowType = rowType;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return typeInformation;
  }
}

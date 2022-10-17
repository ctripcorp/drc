package com.ctrip.framework.drc.applier.mq;

import muise.ctrip.canal.ColumnData;
import muise.ctrip.canal.DataChange;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public abstract class AbstractProducer implements IProducer {

    public DataChange transfer(EventData eventData) {
        DataChange dataChange = new DataChange();
        dataChange.setSchemaName(eventData.getSchemaName());
        dataChange.setTableName(eventData.getTableName());
        dataChange.setEventType(eventData.getEventType().toString());
        List<ColumnData> beforeList = new ArrayList<>();
        List<ColumnData> afterList = new ArrayList<>();
        dataChange.setBeforeColumnList(beforeList);
        dataChange.setAfterColumnList(afterList);
        if (eventData.getBeforeColumns() != null) {
            for (EventColumn column : eventData.getBeforeColumns()) {
                beforeList.add(new ColumnData(column.getColumnName(), column.getColumnValue() == null ? "" : column.getColumnValue(), column.isUpdate(), column.isKey(), column.isNull()));
            }
        }
        if (eventData.getAfterColumns() != null) {
            for (EventColumn column : eventData.getAfterColumns()) {
                afterList.add(new ColumnData(column.getColumnName(), column.getColumnValue() == null ? "" : column.getColumnValue(), column.isUpdate(), column.isKey(), column.isNull()));
            }
        }
        return dataChange;
    }
}

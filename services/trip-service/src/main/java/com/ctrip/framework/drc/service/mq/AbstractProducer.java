package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.Producer;
import com.google.common.collect.Lists;
import muise.ctrip.canal.ColumnData;
import muise.ctrip.canal.DataChange;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by jixinwang on 2022/10/17
 */
public abstract class AbstractProducer implements Producer {

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

    public DataChangeVo transferDataChange(EventData eventData, Set<String> filterFields, boolean excludeColumn) {
        DataChangeVo dataChange = new DataChangeVo();
        dataChange.setSchemaName(eventData.getSchemaName());
        dataChange.setTableName(eventData.getTableName());
        dataChange.setEventType(eventData.getEventType().toString());
        List<DataChangeMessage.ColumnData> beforeList = Lists.newArrayList();
        List<DataChangeMessage.ColumnData> afterList = Lists.newArrayList();
        dataChange.setBeforeColumnList(beforeList);
        dataChange.setAfterColumnList(afterList);
        populateColumnList(eventData.getBeforeColumns(), beforeList, filterFields, excludeColumn);
        populateColumnList(eventData.getAfterColumns(), afterList, filterFields, excludeColumn);
        return dataChange;
    }

    private void populateColumnList(List<EventColumn> columns,
                                    List<DataChangeMessage.ColumnData> targetList,
                                    Set<String> filterFields,
                                    boolean excludeColumn) {
        if (columns != null) {
            for (EventColumn column : columns) {
                if (shouldContain(filterFields, column, excludeColumn)) {
                    targetList.add(new DataChangeMessage.ColumnData(
                            column.getColumnName(),
                            column.getColumnValue() == null ? "" : column.getColumnValue(),
                            column.isUpdate(),
                            column.isKey(),
                            column.isNull()
                    ));
                }
            }
        }
    }

    private boolean shouldContain(Set<String> filterFields, EventColumn column, boolean excludeColumn) {
        if (CollectionUtils.isEmpty(filterFields)) {
            return true;
        }
        boolean contain = filterFields.contains(column.getColumnName().toLowerCase());
        return (!excludeColumn && contain) || (excludeColumn && !contain);
    }
}

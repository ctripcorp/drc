package com.ctrip.framework.drc.core.mq;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public class EventData {

    private String tableName;

    private String schemaName;

    private EventType eventType;

    private List<EventColumn> beforeColumns;

    private List<EventColumn> afterColumns;

    private DcTag dcTag;

    private String orderKey;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public List<EventColumn> getBeforeColumns() {
        return beforeColumns;
    }

    public void setBeforeColumns(List<EventColumn> beforeColumns) {
        this.beforeColumns = beforeColumns;
    }

    public List<EventColumn> getAfterColumns() {
        return afterColumns;
    }

    public void setAfterColumns(List<EventColumn> afterColumns) {
        this.afterColumns = afterColumns;
    }

    public DcTag getDcTag() {
        return dcTag;
    }

    public void setDcTag(DcTag dcTag) {
        this.dcTag = dcTag;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }
}

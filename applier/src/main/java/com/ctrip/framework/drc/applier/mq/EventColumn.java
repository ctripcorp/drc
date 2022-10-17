package com.ctrip.framework.drc.applier.mq;

/**
 * Created by jixinwang on 2022/10/17
 */
public class EventColumn {

    private String columnName;

    private String columnValue;

    private boolean isNull;

    private boolean isKey;

    private boolean isUpdate = true;

    public EventColumn(String columnName, String columnValue, boolean isNull, boolean isKey, boolean isUpdate) {
        this.columnName = columnName;
        this.columnValue = columnValue;
        this.isNull = isNull;
        this.isKey = isKey;
        this.isUpdate = isUpdate;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public void setUpdate(boolean update) {
        isUpdate = update;
    }
}

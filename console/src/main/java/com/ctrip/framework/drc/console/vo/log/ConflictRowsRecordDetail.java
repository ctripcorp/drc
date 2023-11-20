package com.ctrip.framework.drc.console.vo.log;

/**
 * Created by dengquanliang
 * 2023/10/25 14:34
 */
public class ConflictRowsRecordDetail {
    private String tableName;
    private boolean recordIsEqual;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isRecordIsEqual() {
        return recordIsEqual;
    }

    public void setRecordIsEqual(boolean recordIsEqual) {
        this.recordIsEqual = recordIsEqual;
    }
}

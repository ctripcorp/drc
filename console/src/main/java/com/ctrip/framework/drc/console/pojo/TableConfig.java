package com.ctrip.framework.drc.console.pojo;

/**
 * @Author limingdong
 * @create 2021/7/8
 */
public class TableConfig {

    private boolean ignoreReplication;

    private String tableName;

    private String ucsShardColumn;

    public boolean isIgnoreReplication() {
        return ignoreReplication;
    }

    public void setIgnoreReplication(boolean ignoreReplication) {
        this.ignoreReplication = ignoreReplication;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUcsShardColumn() {
        return ucsShardColumn;
    }

    public void setUcsShardColumn(String ucsShardColumn) {
        this.ucsShardColumn = ucsShardColumn;
    }
}

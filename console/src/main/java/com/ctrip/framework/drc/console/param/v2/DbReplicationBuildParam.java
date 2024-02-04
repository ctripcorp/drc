package com.ctrip.framework.drc.console.param.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/31 16:02
 */
public class DbReplicationBuildParam {
    private List<Long> dbReplicationIds;
    private String srcMhaName;
    private String dstMhaName;
    private String dbName;
    private String tableName;
    private boolean flushExistingData;
    private RowsFilterCreateParam rowsFilterCreateParam;
    private ColumnsFilterCreateParam columnsFilterCreateParam;

    public DbReplicationBuildParam(String srcMhaName, String dstMhaName, String dbName, String tableName) {
        this.srcMhaName = srcMhaName;
        this.dstMhaName = dstMhaName;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public DbReplicationBuildParam() {
    }

    public RowsFilterCreateParam getRowsFilterCreateParam() {
        return rowsFilterCreateParam;
    }

    public void setRowsFilterCreateParam(RowsFilterCreateParam rowsFilterCreateParam) {
        this.rowsFilterCreateParam = rowsFilterCreateParam;
    }

    public ColumnsFilterCreateParam getColumnsFilterCreateParam() {
        return columnsFilterCreateParam;
    }

    public void setColumnsFilterCreateParam(ColumnsFilterCreateParam columnsFilterCreateParam) {
        this.columnsFilterCreateParam = columnsFilterCreateParam;
    }

    public List<Long> getDbReplicationIds() {
        return dbReplicationIds;
    }

    public void setDbReplicationIds(List<Long> dbReplicationIds) {
        this.dbReplicationIds = dbReplicationIds;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isFlushExistingData() {
        return flushExistingData;
    }

    public void setFlushExistingData(boolean flushExistingData) {
        this.flushExistingData = flushExistingData;
    }


    @Override
    public String toString() {
        return "DbReplicationBuildParam{" +
                "dbReplicationIds=" + dbReplicationIds +
                ", srcMhaName='" + srcMhaName + '\'' +
                ", dstMhaName='" + dstMhaName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", flushExistingData=" + flushExistingData +
                ", rowsFilterCreateParam=" + rowsFilterCreateParam +
                ", columnsFilterCreateParam=" + columnsFilterCreateParam +
                '}';
    }
}

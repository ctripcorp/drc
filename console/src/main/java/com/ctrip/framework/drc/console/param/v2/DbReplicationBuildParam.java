package com.ctrip.framework.drc.console.param.v2;

import java.util.Collection;
import java.util.List;
import java.util.Set;

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
    private boolean autoBuild;
    private RowsFilterCreateParam rowsFilterCreateParam;
    private ColumnsFilterCreateParam columnsFilterCreateParam;

    public DbReplicationBuildParam(String srcMhaName, String dstMhaName, String dbName, String tableName) {
        this.srcMhaName = srcMhaName;
        this.dstMhaName = dstMhaName;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public boolean isAutoBuild() {
        return autoBuild;
    }

    public void setAutoBuild(boolean autoBuild) {
        this.autoBuild = autoBuild;
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

    public void setDbNames(Collection<String> dbNames) {
        if (dbNames == null || dbNames.isEmpty()) {
            this.dbName = "";
        }else {
            this.dbName = "(" + String.join("|", dbNames) + ")";
        }
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
                ", autoBuild=" + autoBuild +
                ", rowsFilterCreateParam=" + rowsFilterCreateParam +
                ", columnsFilterCreateParam=" + columnsFilterCreateParam +
                '}';
    }
}

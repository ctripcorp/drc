package com.ctrip.framework.drc.console.param.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/1 11:09
 */
public class ColumnsFilterCreateParam {
    private List<Long> dbReplicationIds;
    private int mode;
    private List<String> columns;

    public ColumnsFilterCreateParam(List<Long> dbReplicationIds, int mode, List<String> columns) {
        this.dbReplicationIds = dbReplicationIds;
        this.mode = mode;
        this.columns = columns;
    }

    public ColumnsFilterCreateParam() {
    }

    public List<Long> getDbReplicationIds() {
        return dbReplicationIds;
    }

    public void setDbReplicationIds(List<Long> dbReplicationIds) {
        this.dbReplicationIds = dbReplicationIds;
    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "ColumnsFilterCreateParam{" +
                "dbReplicationIds=" + dbReplicationIds +
                ", mode=" + mode +
                ", columns=" + columns +
                '}';
    }
}

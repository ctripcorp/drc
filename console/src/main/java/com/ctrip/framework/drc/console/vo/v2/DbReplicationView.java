package com.ctrip.framework.drc.console.vo.v2;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/8/2 10:34
 */
public class DbReplicationView {
    private Long dbReplicationId;
    private String dbName;
    private String logicTableName;
    private List<Integer> filterTypes;
    private String createTime;
    private String updateTime;

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Long getDbReplicationId() {
        return dbReplicationId;
    }

    public void setDbReplicationId(Long dbReplicationId) {
        this.dbReplicationId = dbReplicationId;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }

    public List<Integer> getFilterTypes() {
        return filterTypes;
    }

    public void setFilterTypes(List<Integer> filterTypes) {
        this.filterTypes = filterTypes;
    }
}

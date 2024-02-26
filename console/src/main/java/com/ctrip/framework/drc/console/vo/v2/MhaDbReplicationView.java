package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2024/2/22 15:54
 */
public class MhaDbReplicationView extends MhaReplicationView {
    private String dbName;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}

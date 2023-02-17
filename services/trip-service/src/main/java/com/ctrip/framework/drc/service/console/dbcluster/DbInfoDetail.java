package com.ctrip.framework.drc.service.console.dbcluster;

/**
 * @ClassName DbInfoDetail
 * @Author haodongPan
 * @Date 2023/2/15 20:28
 * @Version: $
 */
public class DbInfoDetail {
    
    private String dbNameBase;
    
    private String dbName;

    public String getDbNameBase() {
        return dbNameBase;
    }

    public void setDbNameBase(String dbNameBase) {
        this.dbNameBase = dbNameBase;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}

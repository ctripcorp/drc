package com.ctrip.framework.drc.console.param.v2;

/**
 * Created by dengquanliang
 * 2023/7/31 16:02
 */
public class DbReplicationBuildParam {
    private String srcMhaName;
    private String dstMhaName;
    private String dbName;
    private String tableName;

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

    @Override
    public String toString() {
        return "DbReplicationBuildParam{" +
                "srcMhaName='" + srcMhaName + '\'' +
                ", dstName='" + dstMhaName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}

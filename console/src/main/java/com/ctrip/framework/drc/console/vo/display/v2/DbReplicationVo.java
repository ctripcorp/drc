package com.ctrip.framework.drc.console.vo.display.v2;

/**
 * Created by shiruixin
 * 2024/8/13 10:28
 */
public class DbReplicationVo {
    private Long dbReplicationId;
    private String dbName;
    private String dstLogicTableName;
    private String srcLogicTableName;
    private String mhaName;
    private String dcName;
    private String mqPanelUrl;

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

    public String getDstLogicTableName() {
        return dstLogicTableName;
    }

    public void setDstLogicTableName(String dstLogicTableName) {
        this.dstLogicTableName = dstLogicTableName;
    }

    public String getSrcLogicTableName() {
        return srcLogicTableName;
    }

    public void setSrcLogicTableName(String srcLogicTableName) {
        this.srcLogicTableName = srcLogicTableName;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public String getMqPanelUrl() {
        return mqPanelUrl;
    }

    public void setMqPanelUrl(String mqPanelUrl) {
        this.mqPanelUrl = mqPanelUrl;
    }
}

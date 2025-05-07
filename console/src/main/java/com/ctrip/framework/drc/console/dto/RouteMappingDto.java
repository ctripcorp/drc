package com.ctrip.framework.drc.console.dto;

/**
 * Created by dengquanliang
 * 2025/4/7 16:05
 */
public class RouteMappingDto {

    private long mhaDbReplicationId;
    private long mhaId;
    private String dbName;
    private String srcProxyUris;
    private String relayProxyUris;
    private String dstProxyUris;
    private String routeOrgName;
    private String srcDcName;
    private String dstDcName;

    public long getMhaId() {
        return mhaId;
    }

    public void setMhaId(long mhaId) {
        this.mhaId = mhaId;
    }

    public String getRouteOrgName() {
        return routeOrgName;
    }

    public void setRouteOrgName(String routeOrgName) {
        this.routeOrgName = routeOrgName;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public void setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
    }

    public String getDstDcName() {
        return dstDcName;
    }

    public void setDstDcName(String dstDcName) {
        this.dstDcName = dstDcName;
    }

    public String getSrcProxyUris() {
        return srcProxyUris;
    }

    public void setSrcProxyUris(String srcProxyUris) {
        this.srcProxyUris = srcProxyUris;
    }

    public String getRelayProxyUris() {
        return relayProxyUris;
    }

    public void setRelayProxyUris(String relayProxyUris) {
        this.relayProxyUris = relayProxyUris;
    }

    public String getDstProxyUris() {
        return dstProxyUris;
    }

    public void setDstProxyUris(String dstProxyUris) {
        this.dstProxyUris = dstProxyUris;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public long getMhaDbReplicationId() {
        return mhaDbReplicationId;
    }

    public void setMhaDbReplicationId(long mhaDbReplicationId) {
        this.mhaDbReplicationId = mhaDbReplicationId;
    }
}

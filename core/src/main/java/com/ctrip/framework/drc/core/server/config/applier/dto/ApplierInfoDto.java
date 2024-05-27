package com.ctrip.framework.drc.core.server.config.applier.dto;

import com.ctrip.framework.drc.core.meta.DBInfo;
import com.ctrip.framework.drc.core.server.config.InfoDto;

public class ApplierInfoDto extends InfoDto {
    private String replicatorIp;

    private DBInfo dbInfo;

    public String getReplicatorIp() {
        return replicatorIp;
    }

    public void setReplicatorIp(String replicatorIp) {
        this.replicatorIp = replicatorIp;
    }

    public DBInfo getDbInfo() {
        return dbInfo;
    }

    public void setDbInfo(DBInfo dbInfo) {
        this.dbInfo = dbInfo;
    }

    @Override
    public String getUpstreamIp() {
        return getReplicatorIp();
    }

    @Override
    public void setUpstreamIp(String upstreamIp) {
        setReplicatorIp(upstreamIp);
    }
}

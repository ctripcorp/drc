package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/28 16:35
 */
public class DrcBuildBaseParam {
    private String mhaName;
    private List<String> replicatorIps;
    private List<String> applierIps;
    private List<DbApplierDto> dbApplierDtos;
    private String replicatorInitGtid;
    private String applierInitGtid;

    public DrcBuildBaseParam(String mhaName, List<String> replicatorIps, List<String> applierIps, String replicatorInitGtid, String applierInitGtid) {
        this.mhaName = mhaName;
        this.replicatorIps = replicatorIps;
        this.applierIps = applierIps;
        this.replicatorInitGtid = replicatorInitGtid;
        this.applierInitGtid = applierInitGtid;
    }

    public DrcBuildBaseParam() {
    }

    public List<DbApplierDto> getDbApplierDtos() {
        return dbApplierDtos;
    }

    public void setDbApplierDtos(List<DbApplierDto> dbApplierDtos) {
        this.dbApplierDtos = dbApplierDtos;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<String> getReplicatorIps() {
        return replicatorIps;
    }

    public void setReplicatorIps(List<String> replicatorIps) {
        this.replicatorIps = replicatorIps;
    }

    public List<String> getApplierIps() {
        return applierIps;
    }

    public void setApplierIps(List<String> applierIps) {
        this.applierIps = applierIps;
    }

    public String getReplicatorInitGtid() {
        return replicatorInitGtid;
    }

    public void setReplicatorInitGtid(String replicatorInitGtid) {
        this.replicatorInitGtid = replicatorInitGtid;
    }

    public String getApplierInitGtid() {
        return applierInitGtid;
    }

    public void setApplierInitGtid(String applierInitGtid) {
        this.applierInitGtid = applierInitGtid;
    }

    @Override
    public String toString() {
        return "DrcBuildBaseParam{" +
                "mhaName='" + mhaName + '\'' +
                ", replicatorIps=" + replicatorIps +
                ", applierIps=" + applierIps +
                ", dbApplierDtos=" + dbApplierDtos +
                ", replicatorInitGtid='" + replicatorInitGtid + '\'' +
                ", applierInitGtid='" + applierInitGtid + '\'' +
                '}';
    }
}

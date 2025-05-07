package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.console.dto.v3.DbApplierDto;
import com.ctrip.framework.drc.core.mq.MqType;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/28 16:35
 */
public class DrcBuildBaseParam {
    private String mhaName;
    private List<String> replicatorIps;
    private String replicatorInitGtid;
    private List<DbApplierDto> dbApplierDtos;

    /**
     * only need for mq config
     */
    private String mqType;

    public DrcBuildBaseParam(String mhaName, List<String> replicatorIps, String replicatorInitGtid) {
        this.mhaName = mhaName;
        this.replicatorIps = replicatorIps;
        this.replicatorInitGtid = replicatorInitGtid;
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


    public String getReplicatorInitGtid() {
        return replicatorInitGtid;
    }

    public void setReplicatorInitGtid(String replicatorInitGtid) {
        this.replicatorInitGtid = replicatorInitGtid;
    }

    public String getMqType() {
        return mqType;
    }

    public MqType getMqTypeEnum() {
        return MqType.valueOf(mqType);
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    @Override
    public String toString() {
        return "DrcBuildBaseParam{" +
                "mhaName='" + mhaName + '\'' +
                ", replicatorIps=" + replicatorIps +
                ", replicatorInitGtid='" + replicatorInitGtid + '\'' +
                ", dbApplierDtos=" + dbApplierDtos +
                ", mqType='" + mqType + '\'' +
                '}';
    }
}

package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dao.entity.DbTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;

public class MhaDbDto {
    private Long mhaDbMappingId;
    private String mhaName;
    private String dbName;


    // for front page
    // db related info
    private String buName;
    // mha related info
    private Long dcId;
    private Integer monitorSwitch;
    private String dcName;
    private String regionName;

    public MhaDbDto(Long mhaDbMappingId, String mhaName, String dbName) {
        this.mhaDbMappingId = mhaDbMappingId;
        this.mhaName = mhaName;
        this.dbName = dbName;
    }

    private MhaDbDto() {
    }

    public static MhaDbDto from(Long mhaDbMappingId, MhaTblV2 mhaTbl, DbTbl dbTbl, DcDo dcDo) {
        MhaDbDto dto = new MhaDbDto();
        dto.mhaDbMappingId = mhaDbMappingId;
        dto.dbName = dbTbl.getDbName();
        dto.buName = dbTbl.getBuName();
        dto.mhaName = mhaTbl.getMhaName();
        dto.dcId = mhaTbl.getDcId();
        dto.monitorSwitch = mhaTbl.getMonitorSwitch();
        if (dcDo != null) {
            dto.dcName = dcDo.getDcName();
            dto.regionName = dcDo.getRegionName();
        }
        return dto;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public Long getMhaDbMappingId() {
        return mhaDbMappingId;
    }

    public void setMhaDbMappingId(Long mhaDbMappingId) {
        this.mhaDbMappingId = mhaDbMappingId;
    }


    public Long getDcId() {
        return dcId;
    }

    public Integer getMonitorSwitch() {
        return monitorSwitch;
    }

    public String getBuName() {
        return buName;
    }

    public String getDcName() {
        return dcName;
    }

    public String getRegionName() {
        return regionName;
    }

    @Override
    public String toString() {
        return "MhaDbDto{" +
                "mhaDbMappingId=" + mhaDbMappingId +
                ", mhaName='" + mhaName + '\'' +
                ", dbName='" + dbName + '\'' +
                '}';
    }
}

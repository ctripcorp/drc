package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;

public class MhaDto {
    private Long id;
    private String name;
    private Integer monitorSwitch;
    private Long buId;
    private Long dcId;

    private String dcName;
    private String regionName;


    public static MhaDto from(MhaTblV2 mhaTblV2) {
        MhaDto mhaDto = new MhaDto();
        if (mhaTblV2 != null) {
            mhaDto.setName(mhaTblV2.getMhaName());
            mhaDto.setId(mhaTblV2.getId());
            mhaDto.setMonitorSwitch(mhaTblV2.getMonitorSwitch());
            mhaDto.setBuId(mhaTblV2.getBuId());
            mhaDto.setDcId(mhaTblV2.getDcId());
        }
        return mhaDto;
    }

    public static MhaDto from(MhaTblV2 mhaTblV2, DcDo dcDo) {
        MhaDto mhaDto = new MhaDto();
        if (mhaTblV2 != null) {
            mhaDto.setName(mhaTblV2.getMhaName());
            mhaDto.setId(mhaTblV2.getId());
            mhaDto.setMonitorSwitch(mhaTblV2.getMonitorSwitch());
            mhaDto.setBuId(mhaTblV2.getBuId());
            mhaDto.setDcId(mhaTblV2.getDcId());
        }
        if (dcDo != null) {
            mhaDto.setDcName(dcDo.getDcName());
            mhaDto.setRegionName(dcDo.getRegionName());
        }
        return mhaDto;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getMonitorSwitch() {
        return monitorSwitch;
    }

    public void setMonitorSwitch(Integer monitorSwitch) {
        this.monitorSwitch = monitorSwitch;
    }

    public Long getBuId() {
        return buId;
    }

    public void setBuId(Long buId) {
        this.buId = buId;
    }

    public Long getDcId() {
        return dcId;
    }

    public void setDcId(Long dcId) {
        this.dcId = dcId;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    @Override
    public String toString() {
        return "MhaDto{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }
}

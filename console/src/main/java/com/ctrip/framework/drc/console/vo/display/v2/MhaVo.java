package com.ctrip.framework.drc.console.vo.display.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;

public class MhaVo {
    private Long id;
    private String name;
    private Long buId;
    private Long dcId;
    private Integer monitorSwitch;
    private String buName;
    private String dcName;
    private String regionName;

    public static MhaVo from(MhaTblV2 tbl, DcDo dcDo, BuTbl buTbl) {
        MhaVo vo = new MhaVo();
        if (tbl != null) {
            vo.setId(tbl.getId());
            vo.setName(tbl.getMhaName());
            vo.setBuId(tbl.getBuId());
            vo.setDcId(tbl.getDcId());
            vo.setMonitorSwitch(tbl.getMonitorSwitch());
        }
        if (dcDo != null) {
            vo.setDcName(dcDo.getDcName());
            vo.setRegionName(dcDo.getRegionName());
        } else {
            vo.setDcName("unknown");
            vo.setRegionName("unknown");
        }
        if (buTbl != null) {
            vo.setBuName(buTbl.getBuName());
        } else {
            vo.setBuName("unknown");
        }
        return vo;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Integer getMonitorSwitch() {
        return monitorSwitch;
    }

    public void setMonitorSwitch(Integer monitorSwitch) {
        this.monitorSwitch = monitorSwitch;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
    }

    public String getDcName() {
        return dcName;
    }

    public void setDcName(String dcName) {
        this.dcName = dcName;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }
}

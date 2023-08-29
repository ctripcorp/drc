package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;

public class MhaDto {
    private String name;
    private Long id;
    private Integer monitorSwitch;

    public static MhaDto from(MhaTblV2 mhaTblV2) {
        MhaDto mhaDto = new MhaDto();
        if (mhaTblV2 != null) {
            mhaDto.setName(mhaTblV2.getMhaName());
            mhaDto.setId(mhaTblV2.getId());
            mhaDto.setMonitorSwitch(mhaTblV2.getMonitorSwitch());
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

    @Override
    public String toString() {
        return "MhaDto{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }
}

package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.ReplicatorInfoDto;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;

import java.util.List;
import java.util.Objects;

public class MhaDto {
    private Long id;
    private String name;
    private Integer monitorSwitch;
    private Long buId;
    private Long dcId;

    private String dcName;
    private String regionName;
    private List<MachineDto> machineDtos;
    private List<ReplicatorInfoDto> replicatorInfoDtos;

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

    public static MhaDto from(MhaDbDto mhaDbDto) {
        MhaDto mhaDto = new MhaDto();
        if (mhaDbDto != null) {
            mhaDto.setName(mhaDbDto.getMhaName());
            mhaDto.setDcName(mhaDbDto.getDcName());
            mhaDto.setRegionName(mhaDbDto.getRegionName());
            mhaDto.setMonitorSwitch(mhaDbDto.getMonitorSwitch());
            mhaDto.setDcId(mhaDbDto.getDcId());
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

    public List<MachineDto> getMachineDtos() {
        return machineDtos;
    }

    public void setMachineDtos(List<MachineDto> machineDtos) {
        this.machineDtos = machineDtos;
    }

    public List<ReplicatorInfoDto> getReplicatorInfoDtos() {
        return replicatorInfoDtos;
    }

    public void setReplicatorInfoDtos(List<ReplicatorInfoDto> replicatorInfoDtos) {
        this.replicatorInfoDtos = replicatorInfoDtos;
    }

    @Override
    public String toString() {
        return "MhaDto{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaDto)) return false;
        MhaDto mhaDto = (MhaDto) o;
        return Objects.equals(id, mhaDto.id) && Objects.equals(name, mhaDto.name) && Objects.equals(monitorSwitch, mhaDto.monitorSwitch) && Objects.equals(buId, mhaDto.buId) && Objects.equals(dcId, mhaDto.dcId) && Objects.equals(dcName, mhaDto.dcName) && Objects.equals(regionName, mhaDto.regionName) && Objects.equals(machineDtos, mhaDto.machineDtos) && Objects.equals(replicatorInfoDtos, mhaDto.replicatorInfoDtos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, monitorSwitch, buId, dcId, dcName, regionName, machineDtos, replicatorInfoDtos);
    }
}

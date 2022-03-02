package com.ctrip.framework.drc.console.dto;

import com.google.common.collect.Lists;

import java.util.Objects;

/**
 * @ClassName MhaMachineDto
 * @Author haodongPan
 * @Date 2022/2/21 15:58
 * @Version: $
 */
public class MhaMachineDto {
    private String mhaName;
    
    private MhaInstanceGroupDto.MySQLInstance mySQLInstance;
    
    private boolean master;

    public static MhaInstanceGroupDto transferToMhaInstanceGroupDto (MhaMachineDto dto) {
        MhaInstanceGroupDto sample = new MhaInstanceGroupDto();
        sample.setMhaName(dto.getMhaName());
        if (dto.getMaster()) {
            sample.setMaster(dto.getMySQLInstance());
        } else {
            sample.setSlaves(Lists.newArrayList(dto.getMySQLInstance()));
        }
        return sample;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaMachineDto)) return false;
        MhaMachineDto that = (MhaMachineDto) o;
        return Objects.equals(getMhaName(), that.getMhaName()) && 
                Objects.equals(getMaster(), that.getMaster()) &&
                Objects.equals(getMySQLInstance(), that.getMySQLInstance());
    }

    @Override
    public String toString() {
        return "MhaMachineDto{" +
                "mhaName='" + mhaName + '\'' +
                ", mySQLInstance=" + mySQLInstance + 
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMhaName(), getMySQLInstance());
    }
    
    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public boolean getMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }
    
    public MhaInstanceGroupDto.MySQLInstance getMySQLInstance() {
        return mySQLInstance;
    }

    public void setMySQLInstance(MhaInstanceGroupDto.MySQLInstance mySQLInstance) {
        this.mySQLInstance = mySQLInstance;
    }
}

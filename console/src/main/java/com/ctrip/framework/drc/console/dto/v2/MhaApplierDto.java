package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dto.v3.MhaDbDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;

/**
 * Created by shiruixin
 * 2024/11/15 11:04
 */
public class MhaApplierDto {
    private MhaDbReplicationDto mhaDbReplicationDto;
    private MhaDbDelayInfoDto mhaDbDelayInfoDto;

    public MhaDbReplicationDto getMhaDbReplicationDto() {
        return mhaDbReplicationDto;
    }

    public void setMhaDbReplicationDto(MhaDbReplicationDto mhaDbReplicationDto) {
        this.mhaDbReplicationDto = mhaDbReplicationDto;
    }

    public MhaDbDelayInfoDto getMhaDbDelayInfoDto() {
        return mhaDbDelayInfoDto;
    }

    public void setMhaDbDelayInfoDto(MhaDbDelayInfoDto mhaDbDelayInfoDto) {
        this.mhaDbDelayInfoDto = mhaDbDelayInfoDto;
    }


    public static MhaApplierDto from(MhaDbReplicationDto mhaDbReplicationDto, MhaDbDelayInfoDto mhaDbDelayInfoDto) {
        MhaApplierDto mhaApplierDto = new MhaApplierDto();
        mhaApplierDto.setMhaDbReplicationDto(mhaDbReplicationDto);
        mhaApplierDto.setMhaDbDelayInfoDto(mhaDbDelayInfoDto);
        return mhaApplierDto;
    }

    @Override
    public String toString() {
        return "MhaApplierDto{" +
                "mhaDbReplicationDto=" + mhaDbReplicationDto +
                ", mhaDbDelayInfoDto=" + mhaDbDelayInfoDto +
                '}';
    }
}

package com.ctrip.framework.drc.console.param;

import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;

import java.util.List;

/**
 * Created by dengquanliang
 * 2025/10/21 17:25
 */
public class MhaDbInstanceDto {

    private List<MhaInstanceGroupDto> mhaInstanceGroupDtos;

    public MhaDbInstanceDto(List<MhaInstanceGroupDto> mhaInstanceGroupDtos) {
        this.mhaInstanceGroupDtos = mhaInstanceGroupDtos;
    }

    public MhaDbInstanceDto() {
    }

    public List<MhaInstanceGroupDto> getMhaInstanceGroupDtos() {
        return mhaInstanceGroupDtos;
    }

    public void setMhaInstanceGroupDtos(List<MhaInstanceGroupDto> mhaInstanceGroupDtos) {
        this.mhaInstanceGroupDtos = mhaInstanceGroupDtos;
    }
}

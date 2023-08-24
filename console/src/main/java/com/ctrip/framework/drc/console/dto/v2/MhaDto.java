package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;

public class MhaDto {
    String name;

    public static MhaDto from(MhaTblV2 mhaTblV2) {
        MhaDto mhaDto = new MhaDto();
        if (mhaTblV2 != null) {
            mhaDto.setName(mhaTblV2.getMhaName());
        }
        return mhaDto;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

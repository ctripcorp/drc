package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;

import java.util.HashSet;
import java.util.Set;

public class MhaMessengerDto {

    private Long messengerGroupId;
    private MhaDto srcMha;
    private Set<String> dbs;
    private MhaDelayInfoDto delayInfoDto;
    private Integer status;

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getMessengerGroupId() {
        return messengerGroupId;
    }

    public void setMessengerGroupId(Long messengerGroupId) {
        this.messengerGroupId = messengerGroupId;
    }

    public MhaDto getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(MhaDto srcMha) {
        this.srcMha = srcMha;
    }

    public Set<String> getDbs() {
        return dbs;
    }

    public void setDbs(Set<String> dbs) {
        this.dbs = dbs;
    }

    public MhaDelayInfoDto getDelayInfoDto() {
        return delayInfoDto;
    }

    public void setDelayInfoDto(MhaDelayInfoDto delayInfoDto) {
        this.delayInfoDto = delayInfoDto;
    }

    public static MhaMessengerDto from(MhaTblV2 mhaTblV2, MessengerGroupTbl messengerGroupTbl) {
        MhaMessengerDto dto = new MhaMessengerDto();
        dto.srcMha = MhaDto.from(mhaTblV2);
        dto.messengerGroupId = messengerGroupTbl.getId();
        dto.dbs = new HashSet<>();
        return dto;
    }

    public static MhaMessengerDto from(String srcMhaName) {
        MhaMessengerDto dto = new MhaMessengerDto();

        MhaDto srcMha = new MhaDto();
        srcMha.setName(srcMhaName);
        dto.setSrcMha(srcMha);
        dto.setDbs(new HashSet<>());
        return dto;
    }

    @Override
    public String toString() {
        return "MhaMessengerDto{" +
                "messengerGroupId=" + messengerGroupId +
                ", srcMha=" + srcMha +
                ", dbs=" + dbs +
                ", delayInfoDto=" + delayInfoDto +
                '}';
    }
}

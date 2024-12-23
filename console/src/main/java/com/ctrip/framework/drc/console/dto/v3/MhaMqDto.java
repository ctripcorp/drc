package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dto.v2.MhaDto;

import java.util.List;
import java.util.Objects;

/**
 * for db->mq replication
 */
public class MhaMqDto {
    private MhaDto srcMha;
    private MhaMessengerDto mhaMessengerDto;
    private List<MhaDbReplicationDto> mhaDbReplications;

    public MhaDto getSrcMha() {
        return srcMha;
    }
    public void setSrcMha(MhaDto srcMha) {
        this.srcMha = srcMha;
    }

    public List<MhaDbReplicationDto> getMhaDbReplications() {
        return mhaDbReplications;
    }

    public void setMhaDbReplications(List<MhaDbReplicationDto> mhaDbReplications) {
        this.mhaDbReplications = mhaDbReplications;
    }

    public MhaMessengerDto getMhaMessengerDto() {
        return mhaMessengerDto;
    }

    public void setMhaMessengerDto(MhaMessengerDto mhaMessengerDto) {
        this.mhaMessengerDto = mhaMessengerDto;
    }

    @Override
    public String toString() {
        return "MhaMqDto{" +
                "srcMha=" + srcMha +
                ", mhaMessengerDto=" + mhaMessengerDto +
                ", mhaDbReplications=" + mhaDbReplications +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaMqDto)) return false;
        MhaMqDto mhaMqDto = (MhaMqDto) o;
        return Objects.equals(srcMha, mhaMqDto.srcMha) && Objects.equals(mhaMessengerDto, mhaMqDto.mhaMessengerDto) && Objects.equals(mhaDbReplications, mhaMqDto.mhaDbReplications);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcMha, mhaMessengerDto, mhaDbReplications);
    }
}



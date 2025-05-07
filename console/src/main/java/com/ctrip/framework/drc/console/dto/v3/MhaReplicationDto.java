package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.dto.v2.MhaDto;

import java.util.List;
import java.util.Objects;

/**
 * for db->db replication
 */
public class MhaReplicationDto {
    private MhaDto srcMha;
    private MhaDto dstMha;
    private MhaApplierDto mhaApplierDto;
    private List<MhaDbReplicationDto> mhaDbReplications;

    public MhaDto getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(MhaDto srcMha) {
        this.srcMha = srcMha;
    }

    public MhaDto getDstMha() {
        return dstMha;
    }

    public void setDstMha(MhaDto dstMha) {
        this.dstMha = dstMha;
    }


    public List<MhaDbReplicationDto> getMhaDbReplications() {
        return mhaDbReplications;
    }

    public void setMhaDbReplications(List<MhaDbReplicationDto> mhaDbReplications) {
        this.mhaDbReplications = mhaDbReplications;
    }

    public MhaApplierDto getMhaApplierDto() {
        return mhaApplierDto;
    }

    public void setMhaApplierDto(MhaApplierDto mhaApplierDto) {
        this.mhaApplierDto = mhaApplierDto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaReplicationDto)) return false;
        MhaReplicationDto that = (MhaReplicationDto) o;
        return Objects.equals(srcMha, that.srcMha) && Objects.equals(dstMha, that.dstMha) && Objects.equals(mhaDbReplications, that.mhaDbReplications);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcMha, dstMha, mhaDbReplications);
    }
}



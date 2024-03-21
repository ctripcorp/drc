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
    private List<MhaDbReplicationDto> mhaDbReplications;
    private List<DbApplierDto> dbAppliers;

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

    public List<DbApplierDto> getDbAppliers() {
        return dbAppliers;
    }

    public void setDbAppliers(List<DbApplierDto> dbAppliers) {
        this.dbAppliers = dbAppliers;
    }

    public List<MhaDbReplicationDto> getMhaDbReplications() {
        return mhaDbReplications;
    }

    public void setMhaDbReplications(List<MhaDbReplicationDto> mhaDbReplications) {
        this.mhaDbReplications = mhaDbReplications;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MhaReplicationDto)) return false;
        MhaReplicationDto that = (MhaReplicationDto) o;
        return Objects.equals(srcMha, that.srcMha) && Objects.equals(dstMha, that.dstMha) && Objects.equals(dbAppliers, that.dbAppliers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcMha, dstMha, dbAppliers);
    }
}



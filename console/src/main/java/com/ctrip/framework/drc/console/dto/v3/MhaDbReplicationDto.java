package com.ctrip.framework.drc.console.dto.v3;

public class MhaDbReplicationDto {
    private Long id;
    private MhaDbDto src;
    private MhaDbDto dst;
    private Integer replicationType;
    private Boolean drcStatus;

    public static final MhaDbDto MQ_DTO = new MhaDbDto(-1L, null, null);

    public Integer getReplicationType() {
        return replicationType;
    }

    public void setReplicationType(Integer replicationType) {
        this.replicationType = replicationType;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public MhaDbDto getSrc() {
        return src;
    }

    public void setSrc(MhaDbDto src) {
        this.src = src;
    }

    public MhaDbDto getDst() {
        return dst;
    }

    public void setDst(MhaDbDto dst) {
        this.dst = dst;
    }

    public Boolean getDrcStatus() {
        return drcStatus;
    }

    public void setDrcStatus(Boolean drcStatus) {
        this.drcStatus = drcStatus;
    }

    @Override
    public String toString() {
        return "MhaDbReplicationDto{" +
                "id=" + id +
                ", src=" + src +
                ", dst=" + dst +
                ", replicationType=" + replicationType +
                ", drcStatus=" + drcStatus +
                '}';
    }
}

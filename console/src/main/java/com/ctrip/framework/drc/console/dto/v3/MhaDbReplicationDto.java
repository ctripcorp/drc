package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;

public class MhaDbReplicationDto {
    private Long id;
    private MhaDbDto src;
    private MhaDbDto dst;
    private Integer replicationType;
    private Boolean drcStatus;
    /**
     * @see com.ctrip.framework.drc.console.enums.TransmissionTypeEnum
     */
    private String transmissionType;
    private List<String> logicTable;

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

    public String getTransmissionType() {
        return transmissionType;
    }

    public void setTransmissionType(String transmissionType) {
        this.transmissionType = transmissionType;
    }

    public List<String> getLogicTable() {
        return logicTable;
    }

    public void setLogicTable(List<String> logicTable) {
        this.logicTable = logicTable;
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

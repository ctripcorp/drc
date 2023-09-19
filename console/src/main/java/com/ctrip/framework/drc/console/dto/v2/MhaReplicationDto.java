package com.ctrip.framework.drc.console.dto.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MhaReplicationDto {
    private Long replicationId;
    private MhaDto srcMha;
    private MhaDto dstMha;
    private Set<String> dbs;

    /**
     * @see com.ctrip.framework.drc.console.enums.TransmissionTypeEnum
     */
    private String type;
    private Long datachangeLasttime;
    /**
     * 1: 已建立
     * 0: 未建立
     */
    private Integer status;

    private MhaDelayInfoDto delayInfoDto;

    public static MhaReplicationDto from(MhaReplicationTbl e, Map<Long, MhaTblV2> mhaMap, Map<Long, DcDo> dcDoMap) {
        MhaReplicationDto dto = new MhaReplicationDto();
        dto.setReplicationId(e.getId());
        dto.setDatachangeLasttime(e.getDatachangeLasttime().getTime());
        dto.setStatus(e.getDrcStatus());
        MhaTblV2 srcMhaTbl = mhaMap.get(e.getSrcMhaId());
        MhaTblV2 dstMhaTbl = mhaMap.get(e.getDstMhaId());
        if (srcMhaTbl != null) {
            DcDo dcDo = dcDoMap.get(srcMhaTbl.getDcId());
            dto.setSrcMha(MhaDto.from(srcMhaTbl, dcDo));
        } else {
            dto.setSrcMha(new MhaDto());
        }
        if (dstMhaTbl != null) {
            DcDo dcDo = dcDoMap.get(dstMhaTbl.getDcId());
            dto.setSrcMha(MhaDto.from(dstMhaTbl, dcDo));
        } else {
            dto.setDstMha(new MhaDto());
        }
        return dto;
    }

    public static MhaReplicationDto from(MhaReplicationTbl e, Map<Long, MhaTblV2> mhaMap) {
        MhaReplicationDto dto = new MhaReplicationDto();
        dto.setReplicationId(e.getId());
        dto.setDatachangeLasttime(e.getDatachangeLasttime().getTime());
        dto.setStatus(e.getDrcStatus());
        dto.setSrcMha(MhaDto.from(mhaMap.get(e.getSrcMhaId())));
        dto.setDstMha(MhaDto.from(mhaMap.get(e.getDstMhaId())));
        return dto;
    }

    public static MhaReplicationDto from(MhaReplicationTbl e) {
        MhaReplicationDto dto = new MhaReplicationDto();
        dto.setReplicationId(e.getId());
        dto.setDatachangeLasttime(e.getDatachangeLasttime().getTime());
        dto.setStatus(e.getDrcStatus());
        dto.setDbs(new HashSet<>());

        MhaDto srcMha = new MhaDto();
        srcMha.setId(e.getSrcMhaId());
        dto.setSrcMha(srcMha);

        MhaDto dstMha = new MhaDto();
        dstMha.setId(e.getDstMhaId());
        dto.setDstMha(dstMha);
        return dto;
    }

    public static MhaReplicationDto from(String srcMhaName, String dstMhaName) {
        MhaReplicationDto dto = new MhaReplicationDto();

        MhaDto srcMha = new MhaDto();
        srcMha.setName(srcMhaName);
        dto.setSrcMha(srcMha);

        MhaDto dstMha = new MhaDto();
        dstMha.setName(dstMhaName);
        dto.setDstMha(dstMha);
        return dto;
    }

    public Set<String> getDbs() {
        return dbs;
    }

    public void setDbs(Set<String> dbs) {
        this.dbs = dbs;
    }

    public Long getReplicationId() {
        return replicationId;
    }

    public void setReplicationId(Long replicationId) {
        this.replicationId = replicationId;
    }

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getDatachangeLasttime() {
        return datachangeLasttime;
    }

    public void setDatachangeLasttime(Long datachangeLasttime) {
        this.datachangeLasttime = datachangeLasttime;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public MhaDelayInfoDto getDelayInfoDto() {
        return delayInfoDto;
    }

    public void setDelayInfoDto(MhaDelayInfoDto delayInfoDto) {
        this.delayInfoDto = delayInfoDto;
    }

    @Override
    public String toString() {
        return "MhaReplicationDto{" +
                "replicationId=" + replicationId +
                ", srcMha=" + srcMha +
                ", dstMha=" + dstMha +
                ", dbs=" + dbs +
                ", type='" + type + '\'' +
                ", datachangeLasttime=" + datachangeLasttime +
                ", status=" + status +
                '}';
    }

    public int getSortPriority() {
        int priority = 0;
        if (status != null) {
            priority += 1000 * status;
        }
        return priority;
    }
}

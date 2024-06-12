package com.ctrip.framework.drc.console.param.v2.resource;

/**
 * Created by dengquanliang
 * 2024/6/6 16:23
 */
public class ApplierResourceDto {
    private Long relatedId;
    private Integer type;

    public ApplierResourceDto(Long relatedId, Integer type) {
        this.relatedId = relatedId;
        this.type = type;
    }

    public ApplierResourceDto() {
    }

    public Long getRelatedId() {
        return relatedId;
    }

    public void setRelatedId(Long relatedId) {
        this.relatedId = relatedId;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "ApplierResourceDto{" +
                "relatedId=" + relatedId +
                ", type=" + type +
                '}';
    }
}

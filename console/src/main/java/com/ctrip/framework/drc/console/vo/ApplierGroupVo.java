package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.dao.entity.ApplierGroupTbl;

/**
 * @ClassName ApplierGroupVo
 * @Author haodongPan
 * @Date 2022/5/16 20:29
 * @Version: $
 */
public class ApplierGroupVo {
    String srcMha;
    String destMha;
    String srcDc;
    String destDc;
    String nameFilter;
    String nameMapping;
    Integer mode;
    Long applierGroupId;


    public ApplierGroupVo(String srcMha, String destMha, String srcDc, String destDc,Long applierGroupId) {
        this.srcMha = srcMha;
        this.destMha = destMha;
        this.srcDc = srcDc;
        this.destDc = destDc;
        this.applierGroupId = applierGroupId;
    }

    @Override
    public String toString() {
        return "ApplierGroupVo{" +
                "srcMha='" + srcMha + '\'' +
                ", destMha='" + destMha + '\'' +
                ", srcDc='" + srcDc + '\'' +
                ", destDc='" + destDc + '\'' +
                ", nameFilter='" + nameFilter + '\'' +
                ", nameMapping='" + nameMapping + '\'' +
                ", mode=" + mode +
                ", applierGroupId=" + applierGroupId +
                '}';
    }

    public String getSrcMha() {
        return srcMha;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public String getDestMha() {
        return destMha;
    }

    public void setDestMha(String destMha) {
        this.destMha = destMha;
    }

    public String getSrcDc() {
        return srcDc;
    }

    public void setSrcDc(String srcDc) {
        this.srcDc = srcDc;
    }

    public String getDestDc() {
        return destDc;
    }

    public void setDestDc(String destDc) {
        this.destDc = destDc;
    }

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    public String getNameMapping() {
        return nameMapping;
    }

    public void setNameMapping(String nameMapping) {
        this.nameMapping = nameMapping;
    }

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }

    public Long getApplierGroupId() {
        return applierGroupId;
    }

    public void setApplierGroupId(Long applierGroupId) {
        this.applierGroupId = applierGroupId;
    }
}

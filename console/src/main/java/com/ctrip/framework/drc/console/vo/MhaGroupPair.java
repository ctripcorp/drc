package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-09-24
 */
public class MhaGroupPair {
    private String srcMha;

    private String destMha;

    private Integer drcEstablishStatus;

    private int unitVerificationSwitch;

    private int monitorSwitch;

    private Long mhaGroupId;

    public MhaGroupPair() {
    }

    public MhaGroupPair(String mha1, String mha2, EstablishStatusEnum establishStatusEnum, int unitVerificationSwitch,
                        int monitorSwitch, Long mhaGroupId) {
        if (mha1.compareTo(mha2) < 0) {
            this.srcMha = mha1;
            this.destMha = mha2;
        } else {
            this.srcMha = mha2;
            this.destMha = mha1;
        }
        this.drcEstablishStatus = establishStatusEnum.getCode();
        this.unitVerificationSwitch = unitVerificationSwitch;
        this.monitorSwitch = monitorSwitch;
        this.mhaGroupId = mhaGroupId;
    }

    public String getSrcMha() {
        return srcMha;
    }

    public String getDestMha() {
        return destMha;
    }

    public Integer getDrcEstablishStatus() {
        return drcEstablishStatus;
    }

    public int getUnitVerificationSwitch() {
        return unitVerificationSwitch;
    }

    public void setSrcMha(String srcMha) {
        this.srcMha = srcMha;
    }

    public void setDestMha(String destMha) {
        this.destMha = destMha;
    }

    public void setDrcEstablishStatus(Integer drcEstablishStatus) {
        this.drcEstablishStatus = drcEstablishStatus;
    }

    public void setUnitVerificationSwitch(int unitVerificationSwitch) {
        this.unitVerificationSwitch = unitVerificationSwitch;
    }

    public int getMonitorSwitch() {
        return monitorSwitch;
    }

    public void setMonitorSwitch(int monitorSwitch) {
        this.monitorSwitch = monitorSwitch;
    }

    public Long getMhaGroupId() {
        return mhaGroupId;
    }

    public void setMhaGroupId(Long mhaGroupId) {
        this.mhaGroupId = mhaGroupId;
    }
}

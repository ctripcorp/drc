package com.ctrip.framework.drc.console.vo;

/**
 * @ClassName MhaGroupPairVo
 * @Author haodongPan
 * @Date 2021/8/4 17:41
 * @Version: 1.0$
 */
public class MhaGroupPairVo {
    private String srcMha;

    private String destMha;

    private Integer drcEstablishStatus;

    private int unitVerificationSwitch;

    private int monitorSwitch;

    private Long mhaGroupId;

    public MhaGroupPairVo() {
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

    public Integer getDrcEstablishStatus() {
        return drcEstablishStatus;
    }

    public void setDrcEstablishStatus(Integer drcEstablishStatus) {
        this.drcEstablishStatus = drcEstablishStatus;
    }

    public int getUnitVerificationSwitch() {
        return unitVerificationSwitch;
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

    public MhaGroupPairVo(String srcMha, String destMha, Integer drcEstablishStatus, int unitVerificationSwitch, int monitorSwitch, Long mhaGroupId) {
        this.srcMha = srcMha;
        this.destMha = destMha;
        this.drcEstablishStatus = drcEstablishStatus;
        this.unitVerificationSwitch = unitVerificationSwitch;
        this.monitorSwitch = monitorSwitch;
        this.mhaGroupId = mhaGroupId;
    }
}

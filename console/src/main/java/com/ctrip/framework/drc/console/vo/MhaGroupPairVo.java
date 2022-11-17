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

    @Deprecated
    private int unitVerificationSwitch;

    @Deprecated
    private int monitorSwitch;
    
    private int srcMhaMonitorSwitch;
    
    private int destMhaMonitorSwitch;

    private Long mhaGroupId;
    
    private String type;
    
    private Long BuId;
    
    
    public MhaGroupPairVo exchangeMhaPosition() {
        String tmp = srcMha;
        setSrcMha(destMha);
        setDestMha(tmp);
        
        int tmpSwitch = srcMhaMonitorSwitch;
        setSrcMhaMonitorSwitch(destMhaMonitorSwitch);
        setDestMhaMonitorSwitch(tmpSwitch);
        return this;
    }

    public Long getBuId() {
        return BuId;
    }

    public void setBuId(Long buId) {
        BuId = buId;
    }
   
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public int getSrcMhaMonitorSwitch() {
        return srcMhaMonitorSwitch;
    }

    public void setSrcMhaMonitorSwitch(int srcMhaMonitorSwitch) {
        this.srcMhaMonitorSwitch = srcMhaMonitorSwitch;
    }

    public int getDestMhaMonitorSwitch() {
        return destMhaMonitorSwitch;
    }

    public void setDestMhaMonitorSwitch(int destMhaMonitorSwitch) {
        this.destMhaMonitorSwitch = destMhaMonitorSwitch;
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


    public MhaGroupPairVo() {
    }

    public MhaGroupPairVo(String srcMha, String destMha, Integer drcEstablishStatus, int srcMhaMonitorSwitch, int destMhaMonitorSwitch, Long mhaGroupId) {
        this.srcMha = srcMha;
        this.destMha = destMha;
        this.drcEstablishStatus = drcEstablishStatus;
        this.srcMhaMonitorSwitch = srcMhaMonitorSwitch;
        this.destMhaMonitorSwitch = destMhaMonitorSwitch;
        this.mhaGroupId = mhaGroupId;
    }
}

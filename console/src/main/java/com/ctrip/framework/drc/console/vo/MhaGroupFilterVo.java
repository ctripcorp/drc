package com.ctrip.framework.drc.console.vo;


/**
 * @ClassName MhaDbFilterVo
 * @Author haodongPan
 * @Date 2022/1/11 17:09
 * @Version: $
 */
public class MhaGroupFilterVo {
    
    private String srcMhaName;
    
    private String destMhaName;
    
    private String srcDc;
    
    private String destDc;
    
    private String srcIpPort;
    
    private String destIpPort;
    
    private String srcApplierFilter;
    
    private String destApplierFilter;

    public MhaGroupFilterVo() {
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDestMhaName() {
        return destMhaName;
    }

    public void setDestMhaName(String destMhaName) {
        this.destMhaName = destMhaName;
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

    public String getSrcIpPort() {
        return srcIpPort;
    }

    public void setSrcIpPort(String srcIpPort) {
        this.srcIpPort = srcIpPort;
    }

    public String getDestIpPort() {
        return destIpPort;
    }

    public void setDestIpPort(String destIpPort) {
        this.destIpPort = destIpPort;
    }

    public String getSrcApplierFilter() {
        return srcApplierFilter;
    }

    public void setSrcApplierFilter(String srcApplierFilter) {
        this.srcApplierFilter = srcApplierFilter;
    }

    public String getDestApplierFilter() {
        return destApplierFilter;
    }

    public void setDestApplierFilter(String destApplierFilter) {
        this.destApplierFilter = destApplierFilter;
    }
}

package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2023/9/5 11:53
 */
public class MhaReplicationView {
    private String srcMhaName;
    private String dstMhaName;
    private String srcDcName;
    private String dstDcName;

    public MhaReplicationView() {
    }

    public MhaReplicationView(String srcMhaName, String dstMhaName) {
        this.srcMhaName = srcMhaName;
        this.dstMhaName = dstMhaName;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public String getSrcDcName() {
        return srcDcName;
    }

    public void setSrcDcName(String srcDcName) {
        this.srcDcName = srcDcName;
    }

    public String getDstDcName() {
        return dstDcName;
    }

    public void setDstDcName(String dstDcName) {
        this.dstDcName = dstDcName;
    }
}

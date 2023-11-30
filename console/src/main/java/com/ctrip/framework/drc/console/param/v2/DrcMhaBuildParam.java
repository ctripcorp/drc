package com.ctrip.framework.drc.console.param.v2;

/**
 * Created by dengquanliang
 * 2023/7/27 14:53
 */
public class DrcMhaBuildParam {
    private String srcMhaName;
    private String dstMhaName;
    private String srcDc;
    private String dstDc;
    private String buName;
    private String srcTag;
    private String dstTag;

    public DrcMhaBuildParam(String srcMhaName, String dstMhaName, String srcDc, String dstDc, String buName, String srcTag, String dstTag) {
        this.srcMhaName = srcMhaName;
        this.dstMhaName = dstMhaName;
        this.srcDc = srcDc;
        this.dstDc = dstDc;
        this.buName = buName;
        this.srcTag = srcTag;
        this.dstTag = dstTag;
    }

    public DrcMhaBuildParam() {
    }

    @Override
    public String toString() {
        return "DrcMhaBuildParam{" +
                "srcMhaName='" + srcMhaName + '\'' +
                ", dstMhaName='" + dstMhaName + '\'' +
                ", srcDc='" + srcDc + '\'' +
                ", dstDc='" + dstDc + '\'' +
                ", buName='" + buName + '\'' +
                ", srcTag='" + srcTag + '\'' +
                ", dstTag='" + dstTag + '\'' +
                '}';
    }

    public String getSrcTag() {
        return srcTag;
    }

    public void setSrcTag(String srcTag) {
        this.srcTag = srcTag;
    }

    public String getDstTag() {
        return dstTag;
    }

    public void setDstTag(String dstTag) {
        this.dstTag = dstTag;
    }

    public String getBuName() {
        return buName;
    }

    public void setBuName(String buName) {
        this.buName = buName;
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

    public String getSrcDc() {
        return srcDc;
    }

    public void setSrcDc(String srcDc) {
        this.srcDc = srcDc;
    }

    public String getDstDc() {
        return dstDc;
    }

    public void setDstDc(String dstDc) {
        this.dstDc = dstDc;
    }
}

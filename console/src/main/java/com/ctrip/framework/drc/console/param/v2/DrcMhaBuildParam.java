package com.ctrip.framework.drc.console.param.v2;

/**
 * Created by dengquanliang
 * 2023/7/27 14:53
 */
public class DrcMhaBuildParam {
    private String srcMhaName;
    private String dstMhaName;
    private Long srcDcId;
    private Long dstDcId;
    private Long buId;

    @Override
    public String toString() {
        return "DrcMhaBuildParam{" +
                "srcMhaName='" + srcMhaName + '\'' +
                ", dstMhaName='" + dstMhaName + '\'' +
                ", srcDcId=" + srcDcId +
                ", dstDcId=" + dstDcId +
                ", buId=" + buId +
                '}';
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public Long getSrcDcId() {
        return srcDcId;
    }

    public Long getDstDcId() {
        return dstDcId;
    }

    public Long getBuId() {
        return buId;
    }
}

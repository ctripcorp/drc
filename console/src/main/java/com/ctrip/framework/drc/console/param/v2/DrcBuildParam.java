package com.ctrip.framework.drc.console.param.v2;

/**
 * Created by dengquanliang
 * 2023/7/28 16:40
 */
public class DrcBuildParam {
    private DrcBuildBaseParam srcBuildParam;
    private DrcBuildBaseParam dstBuildParam;

    public DrcBuildBaseParam getSrcBuildParam() {
        return srcBuildParam;
    }

    public void setSrcBuildParam(DrcBuildBaseParam srcBuildParam) {
        this.srcBuildParam = srcBuildParam;
    }

    public DrcBuildBaseParam getDstBuildParam() {
        return dstBuildParam;
    }

    public void setDstBuildParam(DrcBuildBaseParam dstBuildParam) {
        this.dstBuildParam = dstBuildParam;
    }
}

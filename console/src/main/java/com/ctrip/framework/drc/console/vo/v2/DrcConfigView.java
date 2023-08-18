package com.ctrip.framework.drc.console.vo.v2;

/**
 * Created by dengquanliang
 * 2023/8/1 18:19
 */
public class DrcConfigView {
    private DrcMhaConfigView srcMhaConfigView;
    private DrcMhaConfigView dstMhaConfigView;

    public DrcMhaConfigView getSrcMhaConfigView() {
        return srcMhaConfigView;
    }

    public void setSrcMhaConfigView(DrcMhaConfigView srcMhaConfigView) {
        this.srcMhaConfigView = srcMhaConfigView;
    }

    public DrcMhaConfigView getDstMhaConfigView() {
        return dstMhaConfigView;
    }

    public void setDstMhaConfigView(DrcMhaConfigView dstMhaConfigView) {
        this.dstMhaConfigView = dstMhaConfigView;
    }
}

package com.ctrip.framework.drc.console.vo.request;

import java.io.Serializable;
import java.util.List;

/**
 * Created by shiruixin
 * 2025/7/8 14:34
 */
public class UpdateMhaTagDto implements Serializable {
    List<String> mhas;
    List<String> buNames;
    String expectTag;
    boolean showOnly;
    boolean forceChangeTag; //if prev tag is not common. also change to expectTag
    boolean forceSwitch; //force switch applier and messenger


    public List<String> getMhas() {
        return mhas;
    }

    public void setMhas(List<String> mhas) {
        this.mhas = mhas;
    }

    public List<String> getBuNames() {
        return buNames;
    }

    public void setBuNames(List<String> buNames) {
        this.buNames = buNames;
    }

    public String getExpectTag() {
        return expectTag;
    }

    public void setExpectTag(String expectTag) {
        this.expectTag = expectTag;
    }

    public boolean isShowOnly() {
        return showOnly;
    }

    public void setShowOnly(boolean showOnly) {
        this.showOnly = showOnly;
    }

    public boolean isForceChangeTag() {
        return forceChangeTag;
    }

    public void setForceChangeTag(boolean forceTag) {
        this.forceChangeTag = forceTag;
    }

    public boolean isForceSwitch() {
        return forceSwitch;
    }

    public void setForceSwitch(boolean forceSwitch) {
        this.forceSwitch = forceSwitch;
    }
}

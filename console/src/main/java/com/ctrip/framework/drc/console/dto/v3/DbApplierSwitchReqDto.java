package com.ctrip.framework.drc.console.dto.v3;

import java.util.List;

/**
 * @author: yongnian
 * @create: 2024/6/20 11:49
 */
public class DbApplierSwitchReqDto {
    private List<String> dbNames;
    private String srcMhaName;
    private String dstMhaName;
    private boolean switchOnly;

    public List<String> getDbNames() {
        return dbNames;
    }

    public String getSrcMhaName() {
        return srcMhaName;
    }

    public String getDstMhaName() {
        return dstMhaName;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }

    public void setSrcMhaName(String srcMhaName) {
        this.srcMhaName = srcMhaName;
    }

    public void setDstMhaName(String dstMhaName) {
        this.dstMhaName = dstMhaName;
    }

    public boolean isSwitchOnly() {
        return switchOnly;
    }

    public void setSwitchOnly(boolean switchOnly) {
        this.switchOnly = switchOnly;
    }
}

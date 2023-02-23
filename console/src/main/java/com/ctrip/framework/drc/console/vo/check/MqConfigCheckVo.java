package com.ctrip.framework.drc.console.vo.check;

import java.util.List;

/**
 * @ClassName MqConfigCheckVo
 * @Author haodongPan
 * @Date 2023/2/15 17:04
 * @Version: $
 */
public class MqConfigCheckVo {
    
    private boolean allowSubmit;
    
    private List<MqConfigConflictTable> conflictTables;
    
    private String tag;

    public boolean isAllowSubmit() {
        return allowSubmit;
    }

    public void setAllowSubmit(boolean allowSubmit) {
        this.allowSubmit = allowSubmit;
    }

    public List<MqConfigConflictTable> getConflictTables() {
        return conflictTables;
    }

    public void setConflictTables(List<MqConfigConflictTable> conflictTables) {
        this.conflictTables = conflictTables;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}

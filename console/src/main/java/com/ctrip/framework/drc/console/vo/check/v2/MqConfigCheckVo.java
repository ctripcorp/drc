package com.ctrip.framework.drc.console.vo.check.v2;


import org.springframework.util.CollectionUtils;

import java.util.List;


public class MqConfigCheckVo {

    private Boolean allowSubmit = true;

    private List<MqConfigConflictTable> conflictTables;


    public static MqConfigCheckVo from(List<MqConfigConflictTable> conflictTables) {
        MqConfigCheckVo mqConfigCheckVo = new MqConfigCheckVo();
        mqConfigCheckVo.conflictTables = conflictTables;
        mqConfigCheckVo.setAllowSubmit(CollectionUtils.isEmpty(conflictTables));
        return mqConfigCheckVo;
    }

    public Boolean getAllowSubmit() {
        return allowSubmit;
    }

    public void setAllowSubmit(Boolean allowSubmit) {
        this.allowSubmit = allowSubmit;
    }

    public List<MqConfigConflictTable> getConflictTables() {
        return conflictTables;
    }

    public void setConflictTables(List<MqConfigConflictTable> conflictTables) {
        this.conflictTables = conflictTables;
    }
}

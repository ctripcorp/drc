package com.ctrip.framework.drc.console.vo;

import java.util.List;

/**
 * @ClassName DrcBuildPreCheckRes
 * @Author haodongPan
 * @Date 2021/11/16 19:39
 * @Version: $
 */
public class DrcBuildPreCheckVo {
    public static final int CONFLICT = 1;
    public static final int NO_CONFLICT = 0;
    
    private String conflictMha;
    
    private List<String> workingReplicatorIps;
    
    private int status;

    public DrcBuildPreCheckVo() {
    }
    
    public DrcBuildPreCheckVo(String conflictMha, List<String> workingReplicatorIps,int status) {
        this.conflictMha = conflictMha;
        this.workingReplicatorIps = workingReplicatorIps;
        this.status = status;
    }
    
    public List<String> getWorkingReplicatorIps() {
        return workingReplicatorIps;
    }

    public void setWorkingReplicatorIps(List<String> workingReplicatorIps) {
        this.workingReplicatorIps = workingReplicatorIps;
    }
    public String getConflictMha() {
        return conflictMha;
    }

    public void setConflictMha(String conflictMha) {
        this.conflictMha = conflictMha;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}

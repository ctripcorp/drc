package com.ctrip.framework.drc.console.param.v2;

import java.util.List;

/**
 * @ClassName GtidCompensateParam
 * @Author haodongPan
 * @Date 2024/5/21 19:35
 * @Version: $
 */
public class GtidCompensateParam {
    
    private List<Long> srcMhaIds;
    private String srcRegion;
    private boolean execute;

    public List<Long> getSrcMhaIds() {
        return srcMhaIds;
    }

    public void setSrcMhaIds(List<Long> srcMhaIds) {
        this.srcMhaIds = srcMhaIds;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }
    

    public boolean isExecute() {
        return execute;
    }

    public void setExecute(boolean execute) {
        this.execute = execute;
    }

    @Override
    public String toString() {
        return "GtidCompensateParam{" +
                "srcMhaIds=" + srcMhaIds +
                ", srcRegion='" + srcRegion + '\'' +
                ", execute=" + execute +
                '}';
    }
}

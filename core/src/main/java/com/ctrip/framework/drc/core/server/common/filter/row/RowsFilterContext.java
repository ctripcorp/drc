package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import java.util.HashMap;

/**
 * @Author limingdong
 * @create 2022/5/23
 */
public class RowsFilterContext extends HashMap<Object, RowsFilterResult.Status> {

    private String srcRegion;
    private String dstRegion;
    
    private TableMapLogEvent drcTableMapLogEvent;

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }
    

    public TableMapLogEvent getDrcTableMapLogEvent() {
        return drcTableMapLogEvent;
    }

    public void setDrcTableMapLogEvent(TableMapLogEvent tableMapLogEvent) {
        this.drcTableMapLogEvent = tableMapLogEvent;
    }
}

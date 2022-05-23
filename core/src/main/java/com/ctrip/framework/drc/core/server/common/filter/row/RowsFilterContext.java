package com.ctrip.framework.drc.core.server.common.filter.row;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import java.util.HashMap;

/**
 * @Author limingdong
 * @create 2022/5/23
 */
public class RowsFilterContext extends HashMap<String, Boolean> {

    private TableMapLogEvent drcTableMapLogEvent;

    public TableMapLogEvent getDrcTableMapLogEvent() {
        return drcTableMapLogEvent;
    }

    public void setDrcTableMapLogEvent(TableMapLogEvent tableMapLogEvent) {
        this.drcTableMapLogEvent = tableMapLogEvent;
    }
}

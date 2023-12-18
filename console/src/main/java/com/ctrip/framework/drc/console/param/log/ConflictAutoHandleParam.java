package com.ctrip.framework.drc.console.param.log;

import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/11/1 15:15
 */
public class ConflictAutoHandleParam {
    //0-dstMha 1-srcMha
    private Integer writeSide;
    private List<Long> rowLogIds;

    public Integer getWriteSide() {
        return writeSide;
    }

    public void setWriteSide(Integer writeSide) {
        this.writeSide = writeSide;
    }

    public List<Long> getRowLogIds() {
        return rowLogIds;
    }

    public void setRowLogIds(List<Long> rowLogIds) {
        this.rowLogIds = rowLogIds;
    }

    @Override
    public String toString() {
        return "ConflictAutoHandleParam{" +
                "writeSide=" + writeSide +
                ", rowLogIds=" + rowLogIds +
                '}';
    }
}

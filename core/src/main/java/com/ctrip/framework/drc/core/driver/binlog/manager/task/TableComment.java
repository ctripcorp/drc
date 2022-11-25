package com.ctrip.framework.drc.core.driver.binlog.manager.task;

/**
 * @Author limingdong
 * @create 2022/11/25
 */
public class TableComment {

    private String gtids;

    public TableComment() {
    }

    public TableComment(String gtids) {
        this.gtids = gtids;
    }

    public String getGtids() {
        return gtids;
    }

    public void setGtids(String gtids) {
        this.gtids = gtids;
    }

}

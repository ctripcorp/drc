package com.ctrip.framework.drc.core.driver.binlog.manager.task;

/**
 * @Author limingdong
 * @create 2022/11/25
 */
public class TableComment {

    private String gtidSet;

    public TableComment() {
    }

    public TableComment(String gtidSet) {
        this.gtidSet = gtidSet;
    }

    public String getGtidSet() {
        return gtidSet;
    }

    public void setGtidSet(String gtidSet) {
        this.gtidSet = gtidSet;
    }
}

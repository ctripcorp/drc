package com.ctrip.framework.drc.core.driver.binlog.impl;

/**
 * @Author limingdong
 * @create 2022/10/11
 */
public class TransactionContext {

    private boolean isDdl = false;

    private int eventSize;

    public TransactionContext(boolean isDdl) {
        this(isDdl, 1);
    }

    public TransactionContext(boolean isDdl, int eventSize) {
        this.isDdl = isDdl;
        this.eventSize = eventSize;
    }

    public boolean isDdl() {
        return isDdl;
    }

    public void setDdl(boolean ddl) {
        isDdl = ddl;
    }

    public int getEventSize() {
        return eventSize;
    }

    public void setEventSize(int eventSize) {
        this.eventSize = eventSize;
    }
}

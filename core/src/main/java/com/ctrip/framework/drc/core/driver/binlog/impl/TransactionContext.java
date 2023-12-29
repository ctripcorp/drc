package com.ctrip.framework.drc.core.driver.binlog.impl;

/**
 * @Author limingdong
 * @create 2022/10/11
 */
public class TransactionContext {

    private boolean isDdl = false;

    private int eventSize;

    private boolean inBigTransaction = false;

    public TransactionContext(boolean isDdl) {
        this(isDdl, 1);
    }

    public TransactionContext(boolean isDdl, int eventSize) {
        this.isDdl = isDdl;
        this.eventSize = eventSize;
    }

    public TransactionContext(boolean isDdl, int eventSize, boolean inBigTransaction) {
        this.isDdl = isDdl;
        this.eventSize = eventSize;
        this.inBigTransaction = inBigTransaction;
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

    public boolean isInBigTransaction() {
        return inBigTransaction;
    }

    public void setInBigTransaction(boolean inBigTransaction) {
        this.inBigTransaction = inBigTransaction;
    }
}

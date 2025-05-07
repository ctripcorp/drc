package com.ctrip.framework.drc.core.driver.binlog.impl;

/**
 * @Author limingdong
 * @create 2022/10/11
 */
public class TransactionContext {

    private boolean isDdl = false;

    private boolean inBigTransaction = false;

    public TransactionContext(boolean isDdl) {
        this(isDdl, false);
    }

    public TransactionContext(boolean isDdl, boolean inBigTransaction) {
        this.isDdl = isDdl;
        this.inBigTransaction = inBigTransaction;
    }

    public boolean isDdl() {
        return isDdl;
    }

    public void setDdl(boolean ddl) {
        isDdl = ddl;
    }

    public boolean isInBigTransaction(){
        return inBigTransaction;
    }
}

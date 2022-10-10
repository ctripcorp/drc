package com.ctrip.framework.drc.replicator.impl.inbound.filter;

import com.ctrip.framework.drc.core.server.common.filter.Resettable;

/**
 * @Author limingdong
 * @create 2022/9/28
 */
public class TransactionFlags implements Resettable {

    public static final int LOG_EVENT_FILTER_F = 0x00;

    public static final int GTID_F = 0x01;

    public static final int BLACK_TABLE_NAME_F = 0x02;

    public static final int TRANSACTION_TABLE_F = 0x04;

    public static final int OTHER_F = 0x80;

    private int flags = LOG_EVENT_FILTER_F;

    public boolean filtered() {
        return flags != LOG_EVENT_FILTER_F;
    }

    public boolean gtidFiltered() {
        return (flags & GTID_F) == GTID_F;
    }

    public boolean blackTableFiltered() {
        return (flags & BLACK_TABLE_NAME_F) == BLACK_TABLE_NAME_F;
    }

    public boolean transactionTableFiltered() {
        return (flags & TRANSACTION_TABLE_F) == TRANSACTION_TABLE_F;
    }

    public boolean otherFiltered() {
        return (flags & OTHER_F) == OTHER_F;
    }

    public void mark(int flag) {
        flags |= flag;
    }

    public void unmark(int flag) {
        flags &= ~flag;
    }

    @Override
    public void reset() {
        flags = LOG_EVENT_FILTER_F;
    }
}

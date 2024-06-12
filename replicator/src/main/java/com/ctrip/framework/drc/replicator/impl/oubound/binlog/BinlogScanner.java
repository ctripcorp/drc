package com.ctrip.framework.drc.replicator.impl.oubound.binlog;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.List;

/**
 * @author yongnian
 */
public interface BinlogScanner extends Runnable, Lifecycle, Comparable<BinlogScanner> {

    void addSenders(BinlogScanner another);

    List<BinlogSender> getSenders();

    boolean canMerge();

    GtidSet getGtidSet();

    GtidSet getFilteredGtidSet();

    ConsumeType getConsumeType();

    String getName();

    String getCurrentSendingFileName();

    void splitConcernSenders(String schema);
}

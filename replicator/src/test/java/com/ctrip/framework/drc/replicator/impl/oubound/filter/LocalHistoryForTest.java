package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import java.util.List;

public interface LocalHistoryForTest {
    List<OutboundLogEventContext> getHistory(String name);

    String getName();
}

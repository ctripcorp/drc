package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;

import java.util.List;

public interface LocalHistoryForTest {
    List<OutboundLogEventContext> getHistory(String name);

    String getName();

    ConsumeType getConsumeType();
}

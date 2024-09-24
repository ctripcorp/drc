package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.sender.SenderFilterChainContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class LocalSendFilter extends SendFilter implements LocalHistoryForTest {

    public final String name;

    private final ConsumeType consumeType;

    private static final Map<String, List<OutboundLogEventContext>> historyMap = new ConcurrentHashMap<>();

    public LocalSendFilter(SenderFilterChainContext context) {
        super(context);
        this.name = context.getRegisterKey();
        this.consumeType = context.getConsumeType();
        if (!historyMap.containsKey(name)) {
            historyMap.put(name, new ArrayList<>());
        }
    }

    @Override
    protected void doSend(OutboundLogEventContext value) {
        super.doSend(value);
        historyMap.get(name).add(clone(value));
    }

    @Override
    public List<OutboundLogEventContext> getHistory(String name) {
        return historyMap.get(name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ConsumeType getConsumeType() {
        return consumeType;
    }

    private OutboundLogEventContext clone(OutboundLogEventContext value) {
        OutboundLogEventContext clone = new OutboundLogEventContext();
        clone.setEventType(value.eventType);
        clone.setRewrite(value.isRewrite());
        clone.setLogEvent(value.logEvent);
        clone.setCause(value.getCause());
        clone.setSkipEvent(value.isSkipEvent());
        clone.setNoRewrite(value.isNoRewrite());
        clone.setGtid(value.getGtid());
        clone.setEverSeeGtid(value.isEverSeeGtid());
        return clone;
    }


}

package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.xpipe.api.codec.GenericTypeReference;

import java.util.Set;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_uuid_log_event;

/**
 * @Author limingdong
 * @create 2020/12/31
 */
public class DrcUuidLogEvent extends DrcGenericLogEvent<Set<String>> {

    public DrcUuidLogEvent() {
    }

    public DrcUuidLogEvent(Set<String> uuids, int serverId, long currentEventStartPosition) {
        super(uuids, serverId, currentEventStartPosition, drc_uuid_log_event);
    }

    public Set<String> getUuids() {
        return getContent();
    }

    @Override
    protected GenericTypeReference getGenericTypeReference() {
        return new GenericTypeReference<Set<String>>() {};
    }
}

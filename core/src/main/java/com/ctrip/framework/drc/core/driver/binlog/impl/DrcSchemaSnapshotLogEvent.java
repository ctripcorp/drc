package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.xpipe.api.codec.GenericTypeReference;

import java.util.Map;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_schema_snapshot_log_event;

/**
 * @Author limingdong
 * @create 2020/3/9
 */
public class DrcSchemaSnapshotLogEvent extends DrcGenericLogEvent<Map<String, Map<String, String>>> {

    public DrcSchemaSnapshotLogEvent() {
    }

    public DrcSchemaSnapshotLogEvent(Map<String, Map<String, String>> ddls, int serverId, long currentEventStartPosition) {
        super(ddls, serverId, currentEventStartPosition, drc_schema_snapshot_log_event);
    }


    public Map<String, Map<String, String>> getDdls() {
        return getContent();
    }

    @Override
    protected GenericTypeReference getGenericTypeReference() {
        return new GenericTypeReference<Map<String, Map<String, String>>>() {};
    }
}

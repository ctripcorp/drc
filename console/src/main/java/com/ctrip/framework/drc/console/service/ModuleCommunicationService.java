package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.core.entity.Replicator;
import com.ctrip.xpipe.api.endpoint.Endpoint;

public interface ModuleCommunicationService {
    Replicator getActiveReplicator(String dc, String clusterId);

    Endpoint getActiveMySQL(String dc, String clusterId);
}

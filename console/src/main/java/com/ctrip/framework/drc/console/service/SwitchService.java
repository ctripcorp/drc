package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.console.dto.DbEndpointDto;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-03-05
 */
public interface SwitchService {
    ApiResult switchUpdateDb(String cluster, DbEndpointDto dbEndpointDto);

    void switchListenReplicator(String clusterId, String endpoint);
}

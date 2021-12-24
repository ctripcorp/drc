package com.ctrip.framework.drc.core.service.ops;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

public interface OPSApiService extends Ordered {
    JsonNode getAllClusterInfo(String getAllClusterUrl,String accessToken) throws JsonProcessingException;

    JsonNode getAllDbs(String mysqlDbClusterUrl,String accessToken,String clusterName, String env) throws JsonProcessingException;
}

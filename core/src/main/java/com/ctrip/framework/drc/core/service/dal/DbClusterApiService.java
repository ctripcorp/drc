package com.ctrip.framework.drc.core.service.dal;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

public interface DbClusterApiService extends Ordered {

    JsonNode getDalClusterInfo(String dalServicePrefix,String dalClusterName);

    JsonNode getInstanceGroupsInfo(String dalServicePrefix,List<String> mhas);
    
    JsonNode getMhaList(String dalServicePrefix) throws Exception;

    ApiResult switchDalClusterType(String dalClusterName,String dalServicePrefix, DalClusterTypeEnum typeEnum, String zoneId) throws Exception;
    
    JsonNode getIgnoreTableConfigs(String dalClusterUrl,String clusterName);

    JsonNode getMhasNode(String dalClusterUrl,String clusterName);

    JsonNode getDalClusterNode(String dalClusterUrl);

    Map<String, Object> getDalClusterFromDalService(String dalServicePrefix,String clusterName);
    
    JsonNode releaseDalCluster(String dalServicePrefix,String dalClusterName);

    JsonNode registerDalCluster(String dalRegisterPrefix,String requestBody, String goal);

    JsonNode getResultNode(String uri);
}

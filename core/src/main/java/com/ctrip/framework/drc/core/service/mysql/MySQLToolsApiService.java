package com.ctrip.framework.drc.core.service.mysql;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.fasterxml.jackson.databind.JsonNode;

public interface MySQLToolsApiService extends Ordered {
    
    JsonNode applyPreCheck(String mysqlPreCheckUrl,String requestBody);

    JsonNode buildMhaCluster(String buildNewClusterUrl,String buildMhaClusterRequestBody);

    JsonNode buildMhaClusterV2(String buildNewClusterUrl,String buildMhaClusterRequestBody);
    
    JsonNode getCopyResult(String slaveCascadeUrl,String requestBody);

    JsonNode deployDns(String dnsDeployUrl,String requestBody);
    
}

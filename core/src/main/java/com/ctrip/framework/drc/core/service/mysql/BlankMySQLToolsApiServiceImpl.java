package com.ctrip.framework.drc.core.service.mysql;

import com.ctrip.framework.drc.core.service.mysql.MySQLToolsApiService;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * @ClassName BlankMySQLToolsApiServiceImpl
 * @Author haodongPan
 * @Date 2021/12/6 16:44
 * @Version: $
 */
public class BlankMySQLToolsApiServiceImpl implements MySQLToolsApiService {
    
    @Override
    public int getOrder() {
        return 1;
    }

    @Override
    public JsonNode applyPreCheck(String mysqlPreCheckUrl, String requestBody) {
        return null;
    }

    @Override
    public JsonNode buildMhaCluster(String buildNewClusterUrl, String buildMhaClusterRequestBody) {
        return null;
    }

    @Override
    public JsonNode buildMhaClusterV2(String buildNewClusterUrl, String buildMhaClusterRequestBody) {
        return null;
    }

    @Override
    public JsonNode getCopyResult(String slaveCascadeUrl, String requestBody) {
        return null;
    }

    @Override
    public JsonNode deployDns(String dnsDeployUrl, String requestBody) {
        return null;
    }
}

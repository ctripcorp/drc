package com.ctrip.framework.drc.service.console.service;

import com.ctrip.framework.drc.core.service.utils.JacksonUtils;
import com.ctrip.framework.drc.core.service.mysql.MySQLToolsApiService;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * @ClassName MySQLToolsApiServiceImpl
 * @Author haodongPan
 * @Date 2021/11/25 11:46
 * @Version: $
 */

public class MySQLToolsApiServiceImpl implements MySQLToolsApiService {
   

    @Override
    public JsonNode applyPreCheck(String mysqlPreCheckUrl,String requestBody) {
        return JacksonUtils.getRootNode(mysqlPreCheckUrl, requestBody, JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode buildMhaCluster(String buildNewClusterUrl,String buildMhaClusterRequestBody) {
        return JacksonUtils.getRootNode(buildNewClusterUrl, buildMhaClusterRequestBody, JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode buildMhaClusterV2(String buildNewClusterUrl,String buildMhaClusterRequestBody) {
        return JacksonUtils.getRootNode(buildNewClusterUrl, buildMhaClusterRequestBody, JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode getCopyResult(String slaveCascadeUrl,String requestBody) {
        return JacksonUtils.getRootNode(slaveCascadeUrl, requestBody, JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode deployDns(String dnsDeployUrl,String requestBody) {
        return JacksonUtils.getRootNode(dnsDeployUrl, requestBody, JacksonUtils.HTTP_METHOD_POST_TIMEOUT);
    }

    @Override
    public int getOrder() {
        return 0;
    }
}

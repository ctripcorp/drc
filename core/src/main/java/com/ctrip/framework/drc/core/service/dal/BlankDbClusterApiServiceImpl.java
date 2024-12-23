package com.ctrip.framework.drc.core.service.dal;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.service.utils.JacksonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @ClassName BlankDbClusterApiServiceImpl
 * @Author haodongPan
 * @Date 2021/12/6 16:43
 * @Version: $
 */
public class BlankDbClusterApiServiceImpl implements DbClusterApiService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public JsonNode getDalClusterInfo(String dalServicePrefix, String dalClusterName) {
        return null;
    }

    @Override
    public JsonNode getInstanceGroupsInfo(String dalServicePrefix, List<String> mhas) {
        return null;
    }

    @Override
    public JsonNode getMhaList(String dalServicePrefix) {
        return null;
    }

    @Override
    public JsonNode getMhaListAli(String dalServicePrefix) throws Exception {
        return null;
    }

    @Override
    public JsonNode getMhaListAws(String dalServicePrefix) throws Exception {
        return null;
    }

    @Override
    public ApiResult switchDalClusterType(String dalClusterName,String dalServicePrefix, DalClusterTypeEnum typeEnum, String zoneId) throws Exception {
        return null;
    }

    @Override
    public JsonNode getIgnoreTableConfigs(String dalClusterUrl, String clusterName) {
        return null;
    }

    @Override
    public JsonNode getMhasNode(String dalClusterUrl, String clusterName) {
        return null;
    }

    @Override
    public JsonNode getDalClusterNode(String dalClusterUrl) {
        return null;
    }

    @Override
    public Map<String, Object> getDalClusterFromDalService(String dalServicePrefix, String clusterName) {
        return null;
    }

    @Override
    public JsonNode releaseDalCluster(String dalServicePrefix, String dalClusterName) {
        return null;
    }

    @Override
    public JsonNode registerDalCluster(String dalRegisterPrefix, String requestBody, String goal) {
        return null;
    }

    @Override
    public JsonNode getResultNode(String uri) {
        JsonNode result = null;
        try {
            JsonNode rootNode = JacksonUtils.getRootNode(uri, JacksonUtils.HTTP_METHOD_GET);
            result = rootNode.get("result");
        } catch (Exception e) {
            logger.error("Exception, ", e);
        }
        return result;
    }

    @Override
    public String getDalClusterName(String dalServicePrefix, String dbName) {
        return null;
    }


    @Override
    public int getOrder() {
        return 1;
    }
}

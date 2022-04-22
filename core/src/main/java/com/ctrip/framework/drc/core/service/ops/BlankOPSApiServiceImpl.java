package com.ctrip.framework.drc.core.service.ops;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * @ClassName BlankOPSApiServiceImpl
 * @Author haodongPan
 * @Date 2021/12/6 16:46
 * @Version: $
 */
public class BlankOPSApiServiceImpl implements OPSApiService {
    
    @Override
    public JsonNode getAllClusterInfo(String getAllClusterUrl, String accessToken) throws JsonProcessingException {
        return null;
    }

    @Override
    public JsonNode getAllDbs(String mysqlDbClusterUrl, String accessToken, String clusterName, String env) throws JsonProcessingException {
        return null;
    }

    @Override
    public List<AppNode> getAppNodes(String cmsGetServerUrl,String accessToken,List<String> appIds,String env) {
        return null;
    }

    @Override
    public int getOrder() {
        return 1;
    }

    
}

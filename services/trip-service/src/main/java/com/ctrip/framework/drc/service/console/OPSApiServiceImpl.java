package com.ctrip.framework.drc.service.console;

import com.ctrip.framework.drc.core.service.utils.JacksonUtils;
import com.ctrip.framework.drc.core.service.ops.OPSApiService;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @ClassName OPSApiServiceImpl
 * @Author haodongPan
 * @Date 2021/11/25 14:51
 * @Version: $
 */
public class OPSApiServiceImpl  implements OPSApiService {
    
    
    private static final String ACCESS_TOKEN_KEY = "access_token";
    
    private static final String REQUEST_BODY_KEY = "request_body";

    private static final String CLUSTER_NAME_KEY = "cluster_name";

    private static final String ENV_TYPE_KEY = "env_type";

    private static final String DB_TYPE_KEY = "db_type";

    private static final String DB_TYPE_MYSQL = "mysql";

    private static final String DAL_SERVICE_SUFFIX = "?operator=drcAdmin";


    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode getAllClusterInfo(String getAllClusterUrl,String accessToken) throws JsonProcessingException  {
        ObjectNode requestNode = objectMapper.createObjectNode();
        ObjectNode requestBodyNode = objectMapper.createObjectNode();
        requestBodyNode.put(DB_TYPE_KEY, DB_TYPE_MYSQL);
        requestNode.put(ACCESS_TOKEN_KEY, accessToken);
        requestNode.set(REQUEST_BODY_KEY, requestBodyNode);
        String requestBody = objectMapper.writeValueAsString(requestNode);
        return JacksonUtils.getRootNode(getAllClusterUrl, requestBody, JacksonUtils.HTTP_METHOD_POST);
    }

    @Override
    public JsonNode getAllDbs(String mysqlDbClusterUrl,String accessToken,String clusterName, String env) throws JsonProcessingException {
        JsonNode root = null;
        String requestBody = null;
        ObjectNode requestNode = objectMapper.createObjectNode();
        ObjectNode requestBodyNode = objectMapper.createObjectNode();
        requestBodyNode.put(CLUSTER_NAME_KEY, clusterName);
        requestBodyNode.put(ENV_TYPE_KEY, env);
        requestNode.put(ACCESS_TOKEN_KEY, accessToken);
        requestNode.set(REQUEST_BODY_KEY, requestBodyNode);
        requestBody = objectMapper.writeValueAsString(requestNode);
        root = JacksonUtils.getRootNode(mysqlDbClusterUrl, requestBody, JacksonUtils.HTTP_METHOD_POST);
        return root;
    }

    @Override
    public int getOrder() {
        return 0;
    }
}

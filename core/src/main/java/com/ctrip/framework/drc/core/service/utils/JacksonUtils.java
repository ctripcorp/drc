package com.ctrip.framework.drc.core.service.utils;

import com.ctrip.framework.drc.core.http.HttpUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author maojiawei
 * @version 1.0
 * date: 2020-08-10
 */
public class JacksonUtils {


    private static final Logger logger = LoggerFactory.getLogger(JacksonUtils.class);

    private static final ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);

    private static final Integer ERROR_STATUS = 400;

    public static final int HTTP_METHOD_GET = 0 ;

    public static final int HTTP_METHOD_POST = 1 ;

    public static final int HTTP_METHOD_PUT = 2 ;

    public static final int HTTP_METHOD_POST_TIMEOUT = 3 ;

    /**
     * get the response JsonNode
     * method: GET, POST, PUT
     *
     */
    public static JsonNode getRootNode(String uri, String requestBody, int httpMethod){
        JsonNode root = null;
        String response;
        try {
            switch (httpMethod) {
                case HTTP_METHOD_GET: response = HttpUtils.doGet(uri); break;
                case HTTP_METHOD_POST: response = HttpUtils.doPost(uri, requestBody); break;
                case HTTP_METHOD_PUT: response = HttpUtils.doPut(uri, requestBody); break;
                case HTTP_METHOD_POST_TIMEOUT: response = HttpUtils.doPost(uri, requestBody, Constants.twelve * Constants.ten * Constants.oneThousand); break;
                default: response= "{\"fail\": \"Unsupported HTTP method\"}";
            }
            logger.debug("ResponseBody:"+response);
            root = objectMapper.readTree(response);
        } catch (HttpTimeoutException e) {
            logger.error("getRootNodeException",e);
            response = "{\"fail\": \"HTTP connect timed out\"}";
        } catch (JsonParseException e) {
            logger.error("getRootNodeException",e);
            response = "{\"fail\": \"JsonParseException: response body is not a valid Json\"}";
        } catch (Exception e) {
            logger.error("getRootNodeException",e);
            response = "{\"fail\": \"connection failed\"}";
        }
        if(root == null){
            try {
                root = objectMapper.readTree(response);
            } catch (IOException e) {
                logger.error("getRootNodeException",e);
            }
        }
        return root;
    }

    public static JsonNode getRootNode(String uri, int httpMethod){
        return getRootNode(uri, "", httpMethod);
    }

    /**
     * get the response Map
     *
     */
    public static Map<String, Object> getStringObjectMap(JsonNode root) {
        Map<String, Object> res = new HashMap();
        if(root.has("fail")){
            res.put("status", ERROR_STATUS);
            res.put("message", root.get("fail").asText());
        }else {
            res.put("status", root.get("status").asInt());
            res.put("message", root.get("message").asText());
        }
        return res;
    }

    public static String getVal(String requestBody, String key) {
        try {
            JsonNode requestNode = objectMapper.readTree(requestBody);
            JsonNode node = requestNode.get(key);
            return node != null ? node.asText() : null;
        } catch (Throwable t) {
            logger.error("Fail get {} from {}", key, requestBody, t);
            return null;
        }
    }
}

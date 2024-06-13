package com.ctrip.framework.drc.core.http;

import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestOperations;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-07-02
 */
public class HttpUtils {

    private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    protected static RestOperations restTemplate;

    private static HttpHeaders headers;

    public static final int DEFAULT_TIME_OUT = 10000;

    protected static int DEFAULT_MAX_PER_ROUTE = Integer.parseInt(System.getProperty("max-per-route", "1000"));
    protected static int DEFAULT_MAX_TOTAL = Integer.parseInt(System.getProperty("max-per-route", "10000"));
    protected static int DEFAULT_RETRY_TIMES = Integer.parseInt(System.getProperty("retry-times", "1"));
    protected static int DEFAULT_CONNECT_TIMEOUT = Integer.parseInt(System.getProperty("connect-timeout", "1000"));
    public static int DEFAULT_SO_TIMEOUT = Integer.parseInt(System.getProperty("so-timeout", "60000"));
    public static int DEFAULT_RETRY_INTERVAL_MILLI = Integer
            .parseInt(System.getProperty("metaserver.retryIntervalMilli", "5"));

    private static void init() {
        if(null == restTemplate) {
            restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(
                    DEFAULT_MAX_PER_ROUTE,
                    DEFAULT_MAX_TOTAL,
                    DEFAULT_CONNECT_TIMEOUT,
                    DEFAULT_SO_TIMEOUT,
                    DEFAULT_RETRY_TIMES,
                    RetryPolicyFactories.newRestOperationsRetryPolicyFactory(DEFAULT_RETRY_INTERVAL_MILLI));
        }
        if (null == headers) {
            headers = defaultHttpHeaders();
        }
    }

    private static HttpHeaders defaultHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON));
        return headers;
    }

    public static ApiResult get(String url) throws Exception {
        return get(url, ApiResult.class);
    }

    public static ApiResult put(String url, Object body) {
        return put(url, body, ApiResult.class);
    }

    public static <T> T put(String url, Object body, Class<T> clazz, Map<String, ?> urlVariables) {
        init();
        HttpEntity<Object> entity = new HttpEntity<Object>(body, headers);
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.PUT, entity, clazz,urlVariables);
        return response.getBody();
    }

    public static ApiResult post(String url, Object body) {
        return post(url, body, ApiResult.class);
    }
    public static <T> T get(String url, Class<T> responseType, Object... urlVariables) {
        init();
        HttpEntity<Object> requestWithHeader = new HttpEntity<Object>(headers);
        return restTemplate.exchange(url,HttpMethod.GET,requestWithHeader,responseType,urlVariables).getBody();
    }

    public static <T> T get(String url, Class<T> responseType, Map<String, ?> urlVariables) {
        init();
        HttpEntity<Object> requestWithHeader = new HttpEntity<Object>(headers);
        return restTemplate.exchange(url,HttpMethod.GET,requestWithHeader,responseType,urlVariables).getBody();
    }

    public static <T> T put(String url, Object body, Class<T> clazz) {
        init();
        HttpEntity<Object> entity = new HttpEntity<Object>(body, headers);
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.PUT, entity, clazz);
        return response.getBody();
    }

    public static <T> T post(String url, Object body, Class<T> clazz) {
        init();
        HttpEntity<Object> entity = new HttpEntity<Object>(body, headers);
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.POST, entity, clazz);
        return response.getBody();
    }

    public static <T> T post(String url, Map<String, String> header, Object body, Class<T> clazz) {
        init();
        HttpHeaders httpHeaders = defaultHttpHeaders();
        header.forEach(httpHeaders::add);
        HttpEntity<Object> entity = new HttpEntity<Object>(body, httpHeaders);
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.POST, entity, clazz);
        return response.getBody();
    }

    public static <T> T post(String url, Object body, Class<T> clazz,Map<String, ?> urlVariables) {
        init();
        HttpEntity<Object> entity = new HttpEntity<Object>(body, headers);
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.POST, entity, clazz,urlVariables);
        return response.getBody();
    }

    public static <T> T delete(String url, Class<T> clazz) {
        init();
        HttpEntity<Object> entity = new HttpEntity<Object>(headers);
        logger.info("DELETE {}", url);
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.DELETE, entity, clazz);
        return response.getBody();
    }

    public static JsonNode deserialize(String json)  {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            logger.error("deserialize json error: {}", json,e);
            throw new RuntimeException("deserialize json error");
        }
    }
    
    /**
     * Deprecated
     */
    public static String doGet(String uri) throws Exception {
        return doGet(uri, DEFAULT_TIME_OUT);
    }

    /**
     * Deprecated
     */
    public static JsonNode doGetJson(String uri) throws Exception {
        String response = doGet(uri);
        logger.debug("[doGetJson] {}: {}", uri, response);
        return objectMapper.readTree(response);
    }

    /**
     * Deprecated
     */
    public static String doGet(String uri, int timeout) throws Exception {
        DynamicConfig dynamicConfig = DynamicConfig.getInstance();
        String xAccessToken = dynamicConfig.getXAccessToken();
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .header("X-Access-Token", xAccessToken)
                .uri(URI.create(uri))
                .timeout(Duration.ofMillis(timeout))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    /**
     * Deprecated
     */
    public static String doPost(String uri, String requestBody) throws Exception {
        return doPost(uri, requestBody, DEFAULT_TIME_OUT);
    }

    /**
     * Deprecated
     */
    public static JsonNode doPostJson(String uri, String requestBody) throws Exception {
        String response = doPost(uri, requestBody, DEFAULT_TIME_OUT);
        return objectMapper.readTree(response);
    }

    /**
     * Deprecated
     */
    public static String doPost(String uri, String requestBody, int timeout) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .header("Content-Type", "application/json")
                .uri(URI.create(uri))
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofMillis(timeout))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    /**
     * Deprecated
     */
    public static String doPut(String uri, String requestBody) throws Exception {
        return doPut(uri, requestBody, DEFAULT_TIME_OUT);
    }

    /**
     * Deprecated
     */
    public static JsonNode doPutJson(String uri, String requestBody, int timeout) throws Exception {
        String response = doPut(uri, requestBody, timeout);
        return objectMapper.readTree(response);
    }

    /**
     * Deprecated
     */
    public static String doPut(String uri, String requestBody, int timeout) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .header("Content-Type", "application/json")
                .uri(URI.create(uri))
                .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofMillis(timeout))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}

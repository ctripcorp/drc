package com.ctrip.framework.drc.service.console.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.service.ckafka.KafkaApiService;
import com.ctrip.framework.drc.core.service.ckafka.KafkaTopicCreateVo;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.ctrip.framework.drc.service.console.ckafka.KafkaACLCheckResponse;
import com.ctrip.framework.drc.service.console.ckafka.KafkaACLInfo;
import com.ctrip.framework.drc.service.console.ckafka.KafkaValidateTopicResponse;
import com.google.common.collect.Maps;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by shiruixin
 * 2025/4/7 14:03
 */
public class KafkaApiServiceImpl implements KafkaApiService {
    private static final Logger logger = LoggerFactory.getLogger("autoConfig");

    @Override
    public boolean prepareTopic(String accessToken, KafkaTopicCreateVo vo) throws Exception {
        logger.info("start prepare kafka topic: {} ", JSON.toJSONString(vo));
        boolean doExist = validateTopicExistence(accessToken, vo);
        if (doExist) {
            return checkTopicProducePermission(accessToken, vo);
        }
        return createKafkaTopic(accessToken, vo);
    }

    @Override
    public boolean checkTopicProducePermission(String accessToken, KafkaTopicCreateVo createVo) throws Exception {
        logger.info("start checkTopicProducePermission: {} ", JSON.toJSONString(createVo));
        String allRelatedTopicInfoUrl = TripServiceDynamicConfig.getInstance().getCkafkaValidateTopicAclUrl();
        LinkedHashMap<String, Object> requestBodyContent = Maps.newLinkedHashMap();
        requestBodyContent.put("topic", createVo.getTopic());
        requestBodyContent.put("region", createVo.getRegion());
        requestBodyContent.put("origin", createVo.getOrigin());
        requestBodyContent.put("clusterSet", createVo.getClusterSet());
        LinkedHashMap<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("access_token", accessToken);
        requestBody.put("request_body", requestBodyContent);

        String responseStr = post(requestBody, allRelatedTopicInfoUrl);
        KafkaACLCheckResponse response = JsonUtils.fromJson(responseStr, KafkaACLCheckResponse.class);
        if ("FAIL".equalsIgnoreCase(response.getStatus())) {
            logger.error("ValidateTopicACL fail, response: {}", responseStr);
            return false;
        }
        List<KafkaACLInfo> aclInfos = response.getAcls().stream()
                .filter(e -> createVo.getTopic().equals(e.getTopic())).toList();
        if (CollectionUtils.isEmpty(aclInfos)) {
            logger.info("topic {} no acl check. {}", createVo.getTopic(), responseStr);
            return true;
        }
        for (KafkaACLInfo aclInfo : aclInfos) {
            if (!aclInfo.getType().contains("producer")) {
                logger.error("topic {} no producer permission in ckafka. {}", createVo.getTopic(), responseStr);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean createKafkaTopic(String accessToken, KafkaTopicCreateVo createVo) throws Exception {
        logger.info("start createKafkaTopic: {} ", JSON.toJSONString(createVo));
        String url = TripServiceDynamicConfig.getInstance().getCkafkaCreateTopicUrl();
        LinkedHashMap<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("access_token", accessToken);
        requestBody.put("request_body", createVo);
        String responseStr = post(requestBody, url);
        JSONObject jsonObject = JSON.parseObject(responseStr);
        return "SUCCESS".equalsIgnoreCase(jsonObject.getString("status"));
    }

    @Override
    public boolean validateTopicExistence(String accessToken, KafkaTopicCreateVo vo) throws Exception {
        logger.info("start validateTopicExistence: {} ", JSON.toJSONString(vo));
        String url = TripServiceDynamicConfig.getInstance().getCkafkaValidateTopicExistenceUrl();
        LinkedHashMap<String, Object> requestBodyContent = Maps.newLinkedHashMap();
        requestBodyContent.put("topic", vo.getTopic());
        requestBodyContent.put("region", vo.getRegion());
        LinkedHashMap<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("access_token", accessToken);
        requestBody.put("request_body", requestBodyContent);
        String responseStr = post(requestBody, url);
        KafkaValidateTopicResponse response = JsonUtils.fromJson(responseStr, KafkaValidateTopicResponse.class);
        return "SUCCESS".equalsIgnoreCase(response.getStatus());
    }

    private static String post(LinkedHashMap<String, Object> requestBody, String url) throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder().build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, JsonUtils.toJson(requestBody));
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Content-Type", "application/json")
                .build();
        String responseStr;
        try (Response response = client.newCall(request).execute()) {
            responseStr = response.body().string();
            logger.info("ckafka response: {}", responseStr);
        }
        return responseStr;
    }


    @Override
    public int getOrder() {
        return 0;
    }
}

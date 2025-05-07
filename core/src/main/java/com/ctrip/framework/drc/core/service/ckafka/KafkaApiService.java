package com.ctrip.framework.drc.core.service.ckafka;

import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * Created by shiruixin
 * 2025/4/7 14:02
 */
public interface KafkaApiService extends Ordered {
    boolean prepareTopic(String accessToken, KafkaTopicCreateVo vo) throws Exception;

    boolean checkTopicProducePermission(String accessToken, KafkaTopicCreateVo createVo) throws Exception;

    boolean validateTopicExistence(String accessToken, KafkaTopicCreateVo vo) throws Exception;

    boolean createKafkaTopic(String accessToken, KafkaTopicCreateVo kafkaTopicCreateVo) throws Exception;
}

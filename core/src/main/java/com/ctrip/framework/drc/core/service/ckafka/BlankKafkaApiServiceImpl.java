package com.ctrip.framework.drc.core.service.ckafka;

/**
 * Created by shiruixin
 * 2025/4/7 14:03
 */
public class BlankKafkaApiServiceImpl implements KafkaApiService {
    @Override
    public boolean checkTopicProducePermission(String accessToken, KafkaTopicCreateVo createVo) throws Exception {
        return false;
    }

    @Override
    public int getOrder() {
        return 0;
    }

    @Override
    public boolean validateTopicExistence(String accessToken, KafkaTopicCreateVo vo) throws Exception {
        return false;
    }

    @Override
    public boolean createKafkaTopic(String accessToken, KafkaTopicCreateVo kafkaTopicCreateVo) throws Exception {
        return false;
    }

    @Override
    public boolean prepareTopic(String accessToken, KafkaTopicCreateVo vo) throws Exception {
        return false;
    }
}

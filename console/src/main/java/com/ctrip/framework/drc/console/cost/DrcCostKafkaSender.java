package com.ctrip.framework.drc.console.cost;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;

import com.ctrip.framework.drc.console.utils.JsonUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by jixinwang on 2022/9/6
 */
@Component
public class DrcCostKafkaSender {

    private static final String TOPIC = "ops.cost.insight.share.unit.detail.hourly";

    private Producer<String, String> producer;

    @PostConstruct
    private void init() throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "200");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        producer = KafkaClientFactory.newProducer(TOPIC, properties);
    }

    public void send(DrcCostMetric drcCostMetric) {
        producer.send(new ProducerRecord<>(TOPIC, null, JsonUtils.toJson(drcCostMetric)));
    }
}

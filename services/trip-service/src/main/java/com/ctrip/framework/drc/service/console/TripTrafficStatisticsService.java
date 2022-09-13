package com.ctrip.framework.drc.service.console;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.drc.core.service.statistics.traffic.KafKaTrafficMetric;
import com.ctrip.framework.drc.core.service.statistics.traffic.TrafficStatisticsService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by jixinwang on 2022/9/11
 */
public class TripTrafficStatisticsService implements TrafficStatisticsService {


    private static final String TOPIC = "ops.cost.insight.share.unit.detail.hourly";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Producer<String, String> producer;

    public TripTrafficStatisticsService() {
        init();
    }

    private void init() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "200");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        try {
            producer = KafkaClientFactory.newProducer(TOPIC, properties);
        } catch (Throwable t) {
            logger.error("[cost] get kafka producer error", t);
        }
    }

    @Override
    public void send(KafKaTrafficMetric drcCostMetric) {
        String value = JsonUtils.toJson(drcCostMetric);
        producer.send(new ProducerRecord<>(TOPIC, null, JsonUtils.toJson(drcCostMetric)), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("[cost] send value success: {}", value);
                } else {
                    logger.error("[cost] send value error: {}", value, e);
                }
            }
        });
    }

    @Override
    public int getOrder() {
        return 0;
    }
}

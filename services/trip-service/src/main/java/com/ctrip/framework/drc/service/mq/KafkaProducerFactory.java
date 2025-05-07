package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.ckafka.codec.serializer.HermesJsonSerializer;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by dengquanliang
 * 2025/1/9 11:24
 */
public class KafkaProducerFactory {

    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");

    private static Map<String, Producer<String, String>> topicToProducer = Maps.newConcurrentMap();
    private static Map<String, AtomicInteger> refCountMap = Maps.newConcurrentMap();
    private static Map<String, Lock> topicLocks = new ConcurrentHashMap<>();

    public static final String HERMES_CODEC_TYPE = "hermes.codecType";

    public static final String JSON_CODEC_TYPE = "JSON";

    public static Producer<String, String> createProducer(String topic) {
        Lock lock = topicLocks.computeIfAbsent(topic, key -> new ReentrantLock());
        lock.lock();
        try {
            Producer<String, String> producer = MapUtils.getOrCreate(topicToProducer, topic, () -> {
                try {
                    Properties properties = new Properties();
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
                    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HermesJsonSerializer.class.getCanonicalName());
                    if (TripServiceDynamicConfig.getInstance().getAddKafkaCodecTypeSwitch(topic)) {
                        properties.put(HERMES_CODEC_TYPE, JSON_CODEC_TYPE);
                    }
                    properties.put(ProducerConfig.CLIENT_ID_CONFIG, TripServiceDynamicConfig.getInstance().getKafkaAppidToken() + "-drcMessenger");
                    properties.put(ProducerConfig.LINGER_MS_CONFIG, TripServiceDynamicConfig.getInstance().getKafkaLingerMs(topic));
                    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, TripServiceDynamicConfig.getInstance().getKafkaBatchSize(topic));
                    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, TripServiceDynamicConfig.getInstance().getKafkaBufferMemory(topic));
                    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, TripServiceDynamicConfig.getInstance().getKafkaMaxRequestSize(topic));
                    properties.put(ProducerConfig.ACKS_CONFIG, TripServiceDynamicConfig.getInstance().getAcks(topic));
                    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, TripServiceDynamicConfig.getInstance().getCompressionType(topic));

                    Producer<String, String> objectObjectProducer = KafkaClientFactory.newProducer(topic, properties);
                    refCountMap.put(topic, new AtomicInteger(0));
                    return objectObjectProducer;
                } catch (IOException e) {
                    loggerMsg.error("createProducer error, {}", e);
                    throw new RuntimeException("createProducer error," + e);
                }
            });

            int refCount = refCountMap.get(topic).incrementAndGet();
            loggerMsg.info("[KAFKA] topic {}, refCount: {}", topic, refCount);
            return producer;
        } finally {
            lock.unlock();
        }

    }

    public static void destroy(String topic) {
        Lock lock = topicLocks.computeIfAbsent(topic, key -> new ReentrantLock());
        lock.lock();
        try {
            if (refCountMap.get(topic).decrementAndGet() == 0) {
                Producer<String, String> producer = topicToProducer.remove(topic);
                refCountMap.remove(topic);
                producer.close();
                loggerMsg.info("[KAFKA] destroy producer for topic: {}", topic);
            }
        } finally {
            lock.unlock();
        }

    }

    @VisibleForTesting
    protected static int getRefCount(String topic) {
        return refCountMap.get(topic).get();
    }
}

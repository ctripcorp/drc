package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.ckafka.codec.serializer.HermesJsonSerializer;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.dianping.cat.kafka.clients.producer.ProducerConfig;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.Producer;
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
    private static Properties properties = new Properties();

    static {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HermesJsonSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, TripServiceDynamicConfig.getInstance().getKafkaAppidToken() + "-drcMessenger");
    }

    public static Producer<String, String> createProducer(String topic) {
        Lock lock = topicLocks.computeIfAbsent(topic, key -> new ReentrantLock());
        lock.lock();
        try {
            Producer<String, String> producer = MapUtils.getOrCreate(topicToProducer, topic, () -> {
                try {
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

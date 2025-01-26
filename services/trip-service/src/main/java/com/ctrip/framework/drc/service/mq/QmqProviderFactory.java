package com.ctrip.framework.drc.service.mq;

import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by dengquanliang
 * 2024/5/27 16:33
 */
public class QmqProviderFactory {

    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");

    private static Map<String, MessageProducerProvider> topicToProvide = Maps.newConcurrentMap();
    private static Map<String, AtomicInteger> refCountMap = Maps.newConcurrentMap();
    private static Map<String, Lock> topicLocks = new ConcurrentHashMap<>();

    public static MessageProducerProvider createProvider(String topic) {
        Lock lock = topicLocks.computeIfAbsent(topic, key -> new ReentrantLock());
        lock.lock();
        try {
            MessageProducerProvider provider = MapUtils.getOrCreate(topicToProvide, topic, () -> {
                MessageProducerProvider value = new MessageProducerProvider();
                value.init();
                refCountMap.put(topic, new AtomicInteger(0));
                return value;
            });

            int refCount = refCountMap.get(topic).incrementAndGet();
            loggerMsg.info("[MQ] topic {}, refCount: {}", topic, refCount);
            return provider;
        } finally {
            lock.unlock();
        }

    }

    public static void destroy(String topic) {
        Lock lock = topicLocks.computeIfAbsent(topic, key -> new ReentrantLock());
        lock.lock();
        try {
            if (refCountMap.get(topic).decrementAndGet() == 0) {
                MessageProducerProvider provider = topicToProvide.remove(topic);
                refCountMap.remove(topic);
                provider.destroy();
                loggerMsg.info("[MQ] destroy provider for topic: {}", topic);
            }
        } finally {
            lock.unlock();
        }

    }
}

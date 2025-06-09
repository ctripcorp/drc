package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    //<topic <provider, destroyTime>>
    private static Map<String, Pair<MessageProducerProvider, Long>> preCloseProviders = Maps.newConcurrentMap();
    private static final ScheduledExecutorService cleaner = ThreadUtils.newSingleThreadScheduledExecutor("QmqProviderFactory-Cleaner");
    private static final long TTL = TimeUnit.MINUTES.toMillis(1);

    static {
        cleaner.scheduleWithFixedDelay(QmqProviderFactory::destroyIdleProviders, 5, 60, TimeUnit.SECONDS);
    }

    private static void destroyIdleProviders() {
        preCloseProviders.forEach((topic, pair) -> {
            Lock lock = topicLocks.computeIfAbsent(topic, key -> new ReentrantLock());
            lock.lock();
            try {
                if (refCountMap.containsKey(topic) || topicToProvide.containsKey(topic)) {
                    preCloseProviders.remove(topic);
                } else if (System.currentTimeMillis() - pair.getRight() > TTL) {
                    pair.getLeft().destroy();
                    loggerMsg.info("[MQ] destroy provider for topic: {}", topic);
                    preCloseProviders.remove(topic);
                }
            } catch (Exception e) {
                loggerMsg.error("[MQ] UNLIKELY error in destroy provider for topic: {}", topic, e);
            } finally {
                lock.unlock();
            }
        });
    }

    public static MessageProducerProvider createProvider(String topic) {
        Lock lock = topicLocks.computeIfAbsent(topic, key -> new ReentrantLock());
        lock.lock();
        try {
            MessageProducerProvider provider = MapUtils.getOrCreate(topicToProvide, topic, () -> {
                Pair<MessageProducerProvider, Long> cachePair = preCloseProviders.get(topic);
                MessageProducerProvider value;
                if (cachePair != null) {
                    loggerMsg.info("[MQ] topic {} recover from preclose list", topic);
                    value = cachePair.getLeft();
                    preCloseProviders.remove(topic);
                } else {
                    value = new MessageProducerProvider();
                    value.init();
                }
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
                preCloseProviders.put(topic, Pair.of(provider, System.currentTimeMillis()));
                loggerMsg.info("[MQ] predestroy provider for topic: {}", topic);
            }
        } finally {
            lock.unlock();
        }

    }
}

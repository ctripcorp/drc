package com.ctrip.framework.drc.messenger.resource.context;

import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.mq.Producer;
import com.ctrip.framework.drc.fetcher.event.transaction.TransactionData;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.framework.drc.messenger.activity.monitor.MqMetricsActivity.measurementDelay;

/**
 * Created by dengquanliang
 * 2025/5/29 17:00
 */
public class KafkaTransactionContextResource extends MqTransactionContextResource {

    private final Map<String, Pair<Phaser, AtomicInteger>> phaserAndCounters = Maps.newConcurrentMap();

    @Override
    protected void send(List<EventData> eventDatas, EventType eventType, Producer producer) {
        AtomicInteger atomicInteger = activeThreadsMap.computeIfAbsent(registryKey, (key) -> new AtomicInteger(0));
        atomicInteger.getAndIncrement();
        try {
            reportHickWall(eventDatas, System.currentTimeMillis() - logEventHeader.getEventTimestamp() * 1000, measurementDelay, MqType.kafka.name());
            boolean send = producer.sendKafka(eventDatas, eventType, phaserAndCounters.get(fetchGtid()));
            rowsSize.getAndAdd(eventDatas.size());
            reportHickWall(eventDatas, producer.getTopic(), MqType.kafka.name(), send);
        } finally {
            atomicInteger.getAndDecrement();
        }
    }

    @Override
    public void begin() {
        Pair<Phaser, AtomicInteger> phaserAndCounter = MapUtils.getOrCreate(phaserAndCounters, fetchGtid(), () -> Pair.of(new Phaser(), new AtomicInteger(0)));
        phaserAndCounter.getKey().register();
    }

    @Override
    public void disposeResource() {
        for (Map.Entry<String, Pair<Phaser, AtomicInteger>> entry : phaserAndCounters.entrySet()) {
            entry.getValue().getKey().forceTermination();
        }
        phaserAndCounters.clear();
    }

    @Override
    public TransactionData.ApplyResult complete() {
        String gtid = fetchGtid();

        Pair<Phaser, AtomicInteger> phaserAndCounter = phaserAndCounters.get(gtid);
        Phaser phaser = phaserAndCounter.getKey();
        phaser.arriveAndAwaitAdvance();
        phaserAndCounters.remove(gtid);
        if (phaserAndCounter.getValue().get() != 0) {
            logger.error("{} send kafka error: {}", registryKey, gtid);
            throw new RuntimeException("send kafka error");
        }

        return TransactionData.ApplyResult.SUCCESS;
    }
}

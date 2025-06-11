package com.ctrip.framework.drc.core.mq;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jixinwang on 2022/10/17
 */
public interface Producer {

    String getTopic();

    boolean sendQmq(List<EventData> eventDatas, EventType eventType);

    boolean sendKafka(List<EventData> eventDatas, EventType eventType, Pair<Phaser, AtomicInteger> phaserAndCounter);

    void destroy();
}

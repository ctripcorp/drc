package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2025/1/9 14:02
 */
public class KafkaProducer extends AbstractProducer {

    private final String topic;

    private boolean isOrder;

    private String orderKey;

    private List<String> orderKeys;

    private List<String> excludeFilterTypes;

    private Producer<String, String> producer;

    private volatile boolean producerException;

    private static final Logger loggerMsgSend = LoggerFactory.getLogger("KAFKA SEND");

    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");

    private Set<String> filterFields;
    private boolean sendOnlyUpdated;
    private boolean excludeColumn;

    public KafkaProducer(MqConfig mqConfig) {
        this.topic = mqConfig.getTopic();
        this.excludeFilterTypes = mqConfig.getExcludeFilterTypes();
        this.producer = KafkaProducerFactory.createProducer(topic);
        this.isOrder = mqConfig.isOrder();
        this.orderKey = mqConfig.getOrderKey();
        this.orderKeys = StringUtils.isEmpty(this.orderKey)
                ? Lists.newArrayList()
                : Lists.newArrayList(orderKey.toLowerCase().split(","));
        this.filterFields = Optional.ofNullable(mqConfig.getFilterFields()).orElse(Lists.newArrayList())
                .stream().map(String::toLowerCase).collect(Collectors.toSet());
        this.sendOnlyUpdated = mqConfig.isSendOnlyUpdated();
        this.excludeColumn = mqConfig.isExcludeColumn();
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public boolean sendQmq(List<EventData> eventDatas, EventType eventType) {
        return false;
    }

    @Override
    public boolean sendKafka(List<EventData> eventDatas, EventType eventType, Pair<Phaser, AtomicInteger> phaserAndCounter) {
        if (!CollectionUtils.isEmpty(excludeFilterTypes) && excludeFilterTypes.contains(eventType.getValue())) {
            return false;
        }

        Phaser phaser = phaserAndCounter.getKey();
        AtomicInteger counter = phaserAndCounter.getValue();
        for (EventData eventData : eventDatas) {
            Pair<String, String> messagePair = generateMessage(eventData);
            if (messagePair == null) {
                return false;
            }
            phaser.register();
            counter.getAndIncrement();

            String partitionKey = messagePair.getKey();
            String message = messagePair.getValue();
            long start = System.nanoTime();
            producer.send(new ProducerRecord<>(topic, partitionKey, message), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    try {
                        if (e == null) {
                            producerException = false;
                            counter.decrementAndGet();
                            loggerMsgSend.info("[kafka]topic: {} send partitionKey:{},  message: {}, cost:{} us", topic, partitionKey, message, (System.nanoTime() - start) / 1000);
                        } else {
                            loggerMsgSend.error("[kafka]topic: {} send message: {} error", topic, message, e);
                            producerException = true;
                        }
                    } finally {
                        phaser.arriveAndDeregister();
                    }

                }
            });
        }

        if (producerException) {
            throw new RuntimeException(String.format("topic: %s producer error, stop server", topic));
        }
        return true;
    }

    //partitionKey: message
    @VisibleForTesting
    protected Pair<String, String> generateMessage(EventData eventData) {
        String schema = eventData.getSchemaName();
        String table = eventData.getTableName();
        DataChangeVo dataChange = transferDataChange(eventData, filterFields, excludeColumn);

        boolean isChanged = true;
        if (eventData.getEventType() == EventType.UPDATE) {
            isChanged = dataChange.getAfterColumnList().stream().anyMatch(DataChangeMessage.ColumnData::isUpdated);
        }
        if (sendOnlyUpdated && !isChanged) {
            return null;
        }

        DataChangeMessage.OrderKeyInfo orderKeyInfo = new DataChangeMessage.OrderKeyInfo();
        orderKeyInfo.setSchemaName(schema);
        orderKeyInfo.setTableName(table);

        dataChange.setOrderKeyInfo(orderKeyInfo);

        String partitionKey = null;
        List<String> keys = new ArrayList<>();

        List<EventColumn> changedColumns = eventData.getEventType() == EventType.DELETE ? eventData.getBeforeColumns() : eventData.getAfterColumns();
        if (isOrder) {
            if (orderKey == null) {
                keys = changedColumns.stream()
                        .filter(EventColumn::isKey).map(EventColumn::getColumnValue).toList();
                partitionKey = CollectionUtils.isEmpty(keys) ? String.format("%s.%s", schema, table) : String.format("%s.%s_%s", schema, table, String.join("_",keys));
            } else {
                Map<String, EventColumn> orderKeyColumnValues = changedColumns.stream()
                        .filter(column -> orderKeys.contains(column.getColumnName().toLowerCase()))
                        .collect(Collectors.toMap(
                                column -> column.getColumnName().toLowerCase(),
                                column -> column,
                                (existing, replacement) -> replacement
                        ));
                keys = orderKeys.stream()
                        .map(orderKeyColumnValues::get)
                        .filter(Objects::nonNull)
                        .map(EventColumn::getColumnValue).toList();
                if (!CollectionUtils.isEmpty(keys)) {
                    if (keys.size() > 1) {
                        partitionKey = "_" + String.join("_", keys);
                    } else {
                        partitionKey = keys.getFirst();
                    }
                } else {
                    keys = changedColumns.stream()
                            .filter(EventColumn::isKey).map(EventColumn::getColumnValue).toList();
                    String schemaDotTable = String.format("%s.%s", schema, table);
                    loggerMsg.error("[MQ] order key is absent for table: {}", schemaDotTable);
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.order.key.absent", schemaDotTable);
                }
            }
        } else {
            for (EventColumn column : changedColumns) {
                if (column.isKey()) {
                    keys.add(column.getColumnValue());
                }
            }
        }


        orderKeyInfo.setPk(keys);

        long currentTime = System.currentTimeMillis();
        dataChange.setOtterParseTime(currentTime);
        dataChange.setOtterSendTime(currentTime);
        String dataChangeToSend = JSON.toJSONString(dataChange);
        return Pair.of(partitionKey, dataChangeToSend);
    }

    @Override
    public void destroy() {
        KafkaProducerFactory.destroy(topic);
    }
}

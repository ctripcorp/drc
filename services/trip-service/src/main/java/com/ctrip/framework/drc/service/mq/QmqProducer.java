package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.xpipe.utils.VisibleForTesting;
import muise.ctrip.canal.DataChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jixinwang on 2022/10/17
 */
public class QmqProducer extends AbstractProducer {

    private static final Logger loggerMsgSend = LoggerFactory.getLogger("MESSENGER SEND");

    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");

    protected static final String DATA_CHANGE = "dataChange";

    private MessageProducerProvider provider;

    private final String topic;

    private long delayTime;

    private boolean isOrder;

    private String orderKey;

    private AtomicInteger refCount;

    public QmqProducer(MqConfig mqConfig) {
        this.topic = mqConfig.getTopic();
        this.delayTime = mqConfig.getDelayTime();
        this.isOrder = mqConfig.isOrder();
        this.orderKey = mqConfig.getOrderKey();
        this.refCount = new AtomicInteger(1);
        initProvider();
        loggerMsg.info("[MQ] create provider for topic: {}", topic);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.producer.create", topic);
    }

    @Override
    public void increaseRefCount() {
        this.refCount.incrementAndGet();
    }

    @Override
    public int getRefCount() {
        return this.refCount.get();
    }

    private void initProvider() {
        provider = new MessageProducerProvider();
        provider.init();
    }

    @Override
    public void send(List<EventData> eventDatas) {
        for (EventData eventData : eventDatas) {
            Message message = generateMessage(eventData);
            long start = System.nanoTime();
            provider.sendMessage(message);
            loggerMsgSend.info("[[{}]]send messenger cost: {}us, value: {}", topic, (System.nanoTime() - start) / 1000, message.getStringProperty(DATA_CHANGE));
        }
    }

    @VisibleForTesting
    protected Message generateMessage(EventData eventData) {
        String schema = eventData.getSchemaName();
        String table = eventData.getTableName();
        DataChange dataChange = transfer(eventData);
        JSONObject jsonObject = JSON.parseObject(dataChange.toString());
        Message message = provider.generateMessage(topic);

        String dc = eventData.getDcTag().getName();
        message.addTag(dc);
        jsonObject.put("dc", dc);

        if (delayTime > 0) {
            message.setDelayTime(delayTime, TimeUnit.SECONDS);
        }

        Map<String, Object> orderKeyMap = new HashMap<>();
        orderKeyMap.put("schemaName", schema);
        orderKeyMap.put("tableName", table);
        List<String> keys = new ArrayList<>();

        List<EventColumn> changedColumns = eventData.getEventType() == EventType.DELETE ? eventData.getBeforeColumns() : eventData.getAfterColumns();
        if (isOrder) {
            boolean hasOrderKey = false;
            for (EventColumn column : changedColumns) {
                if (column.getColumnName().equalsIgnoreCase(orderKey)) {
                    message.setOrderKey(column.getColumnValue());
                    hasOrderKey = true;
                }
                if (column.isKey()) {
                    keys.add(column.getColumnValue());
                }
            }

            if (!hasOrderKey) {
                String schemaDotTable = String.format("%s.%s", schema, table);
                loggerMsg.error("[MQ] order key is absent for table: {}", schemaDotTable);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.order.key.absent", schemaDotTable);
            }
        } else {
            for (EventColumn column : changedColumns) {
                if (column.isKey()) {
                    keys.add(column.getColumnValue());
                }
            }
        }
        if (eventData.getOrderKey() != null) {
            message.setOrderKey(eventData.getOrderKey());
        }

        orderKeyMap.put("pks", keys);
        jsonObject.put("orderKeyInfo", orderKeyMap);

        long currentTime = System.currentTimeMillis();
        jsonObject.put("otterParseTime", currentTime);
        jsonObject.put("otterSendTime", currentTime);
        jsonObject.put("drcSendTime", currentTime);

        String dataChangeToSend = jsonObject.toJSONString();
        message.setProperty(DATA_CHANGE, dataChangeToSend);
        return message;
    }

    @Override
    public void destroy() {
        if (this.refCount.decrementAndGet() == 0) {
            provider.destroy();
            loggerMsg.info("[MQ] destroy provider for topic: {}", topic);
        }
    }
}

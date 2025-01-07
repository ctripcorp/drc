package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.ctrip.framework.drc.service.mq.listener.MqSendStateListener;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.dianping.cat.Cat;
import muise.ctrip.canal.DataChange;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.dal.DalTransactionProvider;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.MESSENGER_DELAY_MONITOR_TOPIC;
import static qunar.tc.qmq.utils.SubjectBranchUtils.SUB_ENV;

/**
 * Created by jixinwang on 2022/10/17
 */
public class QmqProducer extends AbstractProducer {

    private static final Logger loggerMsgSend = LoggerFactory.getLogger("MESSENGER SEND");

    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");

    protected static final String DATA_CHANGE = "dataChange";

    private MessageProducerProvider provider;

    private final boolean persist;

    private final String topic;

    private long delayTime;

    private boolean isOrder;

    private String orderKey;

    private String qmqTraceSubenv;

    private static boolean subenvSwitch;

    //EventType value: I, U, D
    private List<String> excludeFilterTypes;

    private AtomicReference<String> status = new AtomicReference<>();

    public QmqProducer(MqConfig mqConfig) {
        this.persist = mqConfig.isPersistent();
        this.topic = mqConfig.getTopic();
        this.delayTime = mqConfig.getDelayTime();
        this.isOrder = mqConfig.isOrder();
        this.orderKey = mqConfig.getOrderKey();
        this.qmqTraceSubenv = mqConfig.getSubenv();
        this.excludeFilterTypes = mqConfig.getExcludeFilterTypes();
        this.subenvSwitch = TripServiceDynamicConfig.getInstance().isSubenvEnable();
        init(persist, mqConfig.getPersistentDb());
        loggerMsg.info("[MQ] create provider for topic: {}", topic);
        status.set(Startable.PHASE_NAME_END);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.producer.create", topic);
    }

    private void init(boolean persist, String dalClusterKey) {
        provider = QmqProviderFactory.createProvider(topic);
        if (persist) {
            provider.setTransactionProvider(new DalTransactionProvider(dalClusterKey));
        }
    }

    @Override
    public boolean send(List<EventData> eventDatas, EventType eventType) {
        if (subenvSwitch && !StringUtils.isEmpty(qmqTraceSubenv) && !MESSENGER_DELAY_MONITOR_TOPIC.equals(topic)) {
            Cat.getTraceContext(true).add(SUB_ENV, qmqTraceSubenv);
        }

        if (!CollectionUtils.isEmpty(excludeFilterTypes) && excludeFilterTypes.contains(eventType.getValue())) {
            return false;
        }

        try {
            CountDownLatch latch = new CountDownLatch(eventDatas.size());
            long start = System.nanoTime();

            MqSendStateListener listener = new MqSendStateListener();
            listener.setLatch(latch);
            listener.setProvider(provider);
            listener.setQmqProducer(this);

            for (EventData eventData : eventDatas) {
                Message message = generateMessage(eventData);
                provider.sendMessage(message, listener);
            }
            latch.await();

            loggerMsgSend.info("[[{}]]send messenger cost: {}us, size: {}", topic, (System.nanoTime() - start) / 1000, eventDatas.size());
        } catch (InterruptedException e) {
            loggerMsgSend.error("[Messenger]latch InterruptedException: {}", e.getCause());
        } finally {
            Cat.getTraceContext(true).remove(SUB_ENV);
        }

        return true;
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

        if (persist) {
            message.setStoreAtFailed(true);
        }
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

            if (orderKey == null) {
                String defaultOrderKey = CollectionUtils.isEmpty(keys) ? String.format("%s.%s", schema, table) : String.format("%s.%s_%s", schema, table, String.join("_",keys));
                message.setOrderKey(defaultOrderKey);
                hasOrderKey = true;
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

    public String getTopic() {
        return topic;
    }

    @Override
    public void destroy() {
        status.set(Stoppable.PHASE_NAME_BEGIN);
        QmqProviderFactory.destroy(topic);
        status.set(Stoppable.PHASE_NAME_END);
    }

    public boolean isUsing() {
        return status != null && Startable.PHASE_NAME_END.equals(status.get());
    }
}

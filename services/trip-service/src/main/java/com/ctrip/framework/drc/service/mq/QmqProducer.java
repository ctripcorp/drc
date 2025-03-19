package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.dianping.cat.Cat;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.dal.DalTransactionProvider;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private Set<String> filterFields;
    private boolean sendOnlyUpdated;
    private boolean excludeColumn;

    public QmqProducer(MqConfig mqConfig) {
        this.persist = mqConfig.isPersistent();
        this.topic = mqConfig.getTopic();
        this.delayTime = mqConfig.getDelayTime();
        this.isOrder = mqConfig.isOrder();
        this.orderKey = mqConfig.getOrderKey();
        this.qmqTraceSubenv = mqConfig.getSubenv();
        this.excludeFilterTypes = mqConfig.getExcludeFilterTypes();
        this.subenvSwitch = TripServiceDynamicConfig.getInstance().isSubenvEnable();
        this.filterFields = Optional.ofNullable(mqConfig.getFilterFields()).orElse(Lists.newArrayList())
                        .stream().map(String::toLowerCase).collect(Collectors.toSet());
        this.sendOnlyUpdated = mqConfig.isSendOnlyUpdated();
        this.excludeColumn = mqConfig.isExcludeColumn();
        init(persist, mqConfig.getPersistentDb());
        loggerMsg.info("[MQ] create provider for topic: {}", topic);
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
            for (EventData eventData : eventDatas) {
                Message message = generateMessage(eventData);
                if (message == null) {
                    return false;
                }

                long start = System.nanoTime();
                provider.sendMessage(message, new MessageSendStateListener() {
                    @Override
                    public void onSuccess(Message message) {

                    }

                    @Override
                    public void onFailed(Message message) {
                        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.send.onfail", topic);
                    }
                });
                loggerMsgSend.info("[[{}]]send messenger cost: {}us, value: {}", topic, (System.nanoTime() - start) / 1000, message.getStringProperty(DATA_CHANGE));
            }
        } finally {
            Cat.getTraceContext(true).remove(SUB_ENV);
        }

        return true;
    }

    /**
     * return null when no fields have been modified and sendOnlyUpdated is configured.
     */
    @VisibleForTesting
    protected Message generateMessage(EventData eventData) {
        String schema = eventData.getSchemaName();
        String table = eventData.getTableName();
        DataChangeVo dataChange = transferDataChange(eventData,filterFields, excludeColumn);

        boolean isChanged = true;
        if (eventData.getEventType() == EventType.UPDATE) {
            isChanged = dataChange.getAfterColumnList().stream().anyMatch(DataChangeMessage.ColumnData :: isUpdated);
        }
        if (sendOnlyUpdated && !isChanged) {
            return null;
        }
        Message message = provider.generateMessage(topic);
        String dc = eventData.getDcTag().getName();
        message.addTag(dc);
        if (persist) {
            message.setStoreAtFailed(true);
        }
        if (delayTime > 0) {
            message.setDelayTime(delayTime, TimeUnit.SECONDS);
        }

        dataChange.setDc(dc);

        DataChangeMessage.OrderKeyInfo orderKeyInfo = new DataChangeMessage.OrderKeyInfo();
        orderKeyInfo.setSchemaName(schema);
        orderKeyInfo.setTableName(table);
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
        orderKeyInfo.setPk(keys);
        dataChange.setOrderKeyInfo(orderKeyInfo);


        long currentTime = System.currentTimeMillis();
        dataChange.setOtterParseTime(currentTime);
        dataChange.setOtterSendTime(currentTime);
        dataChange.setDrcSendTime(currentTime);

        String dataChangeToSend = JSON.toJSONString(dataChange);
        message.setProperty(DATA_CHANGE, dataChangeToSend);
        return message;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public void destroy() {
        QmqProviderFactory.destroy(topic);
    }

}

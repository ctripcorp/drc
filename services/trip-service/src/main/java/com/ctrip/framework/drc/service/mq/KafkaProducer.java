package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import muise.ctrip.canal.DataChange;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2025/1/9 14:02
 */
public class KafkaProducer extends AbstractProducer {

    private final String topic;

    private boolean isOrder;

    private String orderKey;

    private List<String> excludeFilterTypes;

    private Producer<String, String> producer;

    private volatile boolean producerException;

    private static final Logger loggerMsgSend = LoggerFactory.getLogger("MESSENGER SEND");

    private static final Logger loggerMsg = LoggerFactory.getLogger("MESSENGER");

    private boolean cpuOptimizeSwitch;
    private boolean cpuCompareSwitch;

    public KafkaProducer(MqConfig mqConfig) {
        this.topic = mqConfig.getTopic();
        this.excludeFilterTypes = mqConfig.getExcludeFilterTypes();
        this.producer = KafkaProducerFactory.createProducer(topic);
        this.isOrder = mqConfig.isOrder();
        this.orderKey = mqConfig.getOrderKey();
        this.cpuOptimizeSwitch = TripServiceDynamicConfig.getInstance().isCpuOptimizeEnable(topic);
        this.cpuCompareSwitch = TripServiceDynamicConfig.getInstance().isCpuOptimizeCompareModeEnable(topic);
    }

    @Override
    public String getTopic() {
        return topic;
    }

    protected String removeTimestamps(String jsonStr) {
        String pattern = "(\"otterSendTime\"|\"otterParseTime\"|\"drcSendTime\"):\\d+";
        return jsonStr.replaceAll(pattern, "$1:\"\"");
    }

    @Override
    public boolean send(List<EventData> eventDatas, EventType eventType) {
        if (producerException) {
            throw new RuntimeException(String.format("topic: %s producer error, stop server", topic));
        }

        if (!CollectionUtils.isEmpty(excludeFilterTypes) && excludeFilterTypes.contains(eventType.getValue())) {
            return false;
        }
        for (EventData eventData : eventDatas) {
            Pair<String, String> messagePair;
            if (cpuCompareSwitch) {
                messagePair = generateMessageOld(eventData);
                Pair<String, String> messagePairNew = generateMessage(eventData);
                String jsonOld = removeTimestamps(messagePair.getValue());
                String jsonNew = removeTimestamps(messagePairNew.getValue());
                if (!jsonOld.equals(jsonNew)) {
                    DefaultEventMonitorHolder.getInstance().logEvent("DRC.mq.json.compare.different", topic);
                    loggerMsgSend.error("[JSON COMPARE FAIL,KAFKA] topic:{}, old:{}, new:{}", topic, jsonOld, jsonNew);
                }
            } else {
                if (cpuOptimizeSwitch) {
                    messagePair = generateMessage(eventData);
                } else {
                    messagePair = generateMessageOld(eventData);
                }
            }

            String partitionKey = messagePair.getKey();
            String message = messagePair.getValue();
            producer.send(new ProducerRecord<>(topic, partitionKey, message), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        loggerMsgSend.info("[kafka]topic: {} send partitionKey:{},  message: {}", topic, partitionKey, message);
                    } else {
                        loggerMsgSend.error("[kafka]topic: {} send message: {} error", topic, message, e);
                        producerException = true;
                    }
                }
            });


        }
        return true;
    }

    //partitionKey: message
    @VisibleForTesting
    protected Pair<String, String> generateMessageOld(EventData eventData) {
        String schema = eventData.getSchemaName();
        String table = eventData.getTableName();
        DataChange dataChange = transfer(eventData);
        JSONObject jsonObject = JSON.parseObject(dataChange.toString());

        Map<String, Object> orderKeyMap = new HashMap<>();
        orderKeyMap.put("schemaName", schema);
        orderKeyMap.put("tableName", table);
        jsonObject.put("orderKeyInfo", orderKeyMap);

        String partitionKey = null;
        List<String> keys = new ArrayList<>();

        List<EventColumn> changedColumns = eventData.getEventType() == EventType.DELETE ? eventData.getBeforeColumns() : eventData.getAfterColumns();
        if (isOrder) {
            boolean hasOrderKey = false;
            for (EventColumn column : changedColumns) {
                if (column.getColumnName().equalsIgnoreCase(orderKey)) {
                    partitionKey = column.getColumnValue();
                    hasOrderKey = true;
                }
                if (column.isKey()) {
                    keys.add(column.getColumnValue());
                }
            }

            if (!hasOrderKey) {
                String schemaDotTable = String.format("%s.%s", schema, table);
                loggerMsg.error("[KAFKA] order key is absent for table: {}", schemaDotTable);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.kafka.order.key.absent", schemaDotTable);
            }
        } else {
            for (EventColumn column : changedColumns) {
                if (column.isKey()) {
                    keys.add(column.getColumnValue());
                }
            }
        }

        orderKeyMap.put("pks", keys);

        long currentTime = System.currentTimeMillis();
        jsonObject.put("otterParseTime", currentTime);
        jsonObject.put("otterSendTime", currentTime);
        return Pair.of(partitionKey, jsonObject.toJSONString());
    }

    @VisibleForTesting
    protected Pair<String, String> generateMessage(EventData eventData) {
        String schema = eventData.getSchemaName();
        String table = eventData.getTableName();
        DataChangeVo dataChange = transferDataChange(eventData);

        DataChangeMessage.OrderKeyInfo orderKeyInfo = new DataChangeMessage.OrderKeyInfo();
        orderKeyInfo.setSchemaName(schema);
        orderKeyInfo.setTableName(table);

        dataChange.setOrderKeyInfo(orderKeyInfo);

        String partitionKey = null;
        List<String> keys = new ArrayList<>();

        List<EventColumn> changedColumns = eventData.getEventType() == EventType.DELETE ? eventData.getBeforeColumns() : eventData.getAfterColumns();
        if (isOrder) {
            boolean hasOrderKey = false;
            for (EventColumn column : changedColumns) {
                if (column.getColumnName().equalsIgnoreCase(orderKey)) {
                    partitionKey = column.getColumnValue();
                    hasOrderKey = true;
                }
                if (column.isKey()) {
                    keys.add(column.getColumnValue());
                }
            }

            if (!hasOrderKey) {
                String schemaDotTable = String.format("%s.%s", schema, table);
                loggerMsg.error("[KAFKA] order key is absent for table: {}", schemaDotTable);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.kafka.order.key.absent", schemaDotTable);
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

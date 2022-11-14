package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import com.ctrip.framework.drc.core.mq.EventType;
import muise.ctrip.canal.DataChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.dal.DalTransactionProvider;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by jixinwang on 2022/10/17
 */
public class QmqProducer extends AbstractProducer {

    private static final Logger loggerMsgSend = LoggerFactory.getLogger("MESSENGER SEND");

    private MessageProducerProvider provider;

    private final boolean persist;

    private final String topic;

    private long delayTime;

    private boolean isOrder;

    private String orderKey;

    public QmqProducer(MqConfig mqConfig) {
        this.persist = mqConfig.isPersistent();
        this.topic = mqConfig.getTopic();
        this.delayTime = mqConfig.getDelayTime();
        this.isOrder = mqConfig.isOrder();
        this.orderKey = mqConfig.getOrderKey();
        init(persist, mqConfig.getPersistentDb());
    }

    private void init(boolean persist, String dalClusterKey) {
        provider = new MessageProducerProvider();
        provider.init();
        if (persist) {
            provider.setTransactionProvider(new DalTransactionProvider(dalClusterKey));
        }
    }

    @Override
    public void send(List<EventData> eventDatas) {
        for (EventData eventData : eventDatas) {
            DataChange dataChange = transfer(eventData);
            JSONObject jsonObject = JSON.parseObject(dataChange.toString());
            Message message = provider.generateMessage(topic);

            message.addTag(eventData.getDcTag().getName());

            if (persist) {
                message.setStoreAtFailed(true);
            }
            if (delayTime > 0) {
                message.setDelayTime(delayTime, TimeUnit.SECONDS);
            }

            Map<String, Object> orderKeyMap = new HashMap<>();
            orderKeyMap.put("schemaName", eventData.getSchemaName());
            orderKeyMap.put("tableName", eventData.getTableName());
            List<String> keys = new ArrayList<>();

            List<EventColumn> changedColumns = eventData.getEventType() == EventType.UPDATE ? eventData.getAfterColumns() : eventData.getBeforeColumns();
            if (isOrder) {
                for (EventColumn column : changedColumns) {
                    if (column.getColumnName().equalsIgnoreCase(orderKey)) {
                        message.setOrderKey(column.getColumnValue());
                    }
                    if (column.isKey()) {
                        keys.add(column.getColumnValue());
                    }
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
            message.setProperty("dataChange", dataChangeToSend);

            long start = System.currentTimeMillis();
            provider.sendMessage(message);
            loggerMsgSend.info("[[{}]]send messenger cost: {}ms, value: {}", topic, System.currentTimeMillis() - start, dataChangeToSend);
        }
    }

    @Override
    public void destroy() {
        provider.destroy();
    }
}

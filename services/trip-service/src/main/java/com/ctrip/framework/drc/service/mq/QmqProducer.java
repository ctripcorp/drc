package com.ctrip.framework.drc.service.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.mq.EventColumn;
import com.ctrip.framework.drc.core.mq.EventData;
import muise.ctrip.canal.DataChange;
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

    private MessageProducerProvider provider;

    private final boolean persist;

    private final String topic;

    private long delayTime;

    public QmqProducer(MqConfig mqConfig) {
        this.persist = mqConfig.isPersistent();
        this.topic = mqConfig.getTopic();
        this.delayTime = mqConfig.getDelayTime();
        init(persist, mqConfig.getPersistentDb());
    }

    private void init(boolean persist, String realTitanKey) {
        provider = new MessageProducerProvider();
        provider.init();
        if (persist) {
            provider.setTransactionProvider(new DalTransactionProvider(realTitanKey));
        }
    }

    @Override
    public void send(List<EventData> eventDatas) {
        for (EventData eventData : eventDatas) {
            DataChange dataChange = transfer(eventData);
            JSONObject jsonObject = JSON.parseObject(dataChange.toString());
            Message message = provider.generateMessage(topic);

            if(persist){
                message.setStoreAtFailed(true);
            }

            if (eventData.getOrderKey() != null) {
                message.setOrderKey(eventData.getOrderKey());
            }
            Map<String, Object> orderKeyMap = new HashMap<>();
            orderKeyMap.put("schemaName", eventData.getSchemaName());
            orderKeyMap.put("tableName", eventData.getTableName());
            List<String> keys = new ArrayList<>();
            for (EventColumn column : eventData.getKeys()) {
                keys.add(column.getColumnValue());
            }
            orderKeyMap.put("pks", keys);
            jsonObject.put("orderKeyInfo", orderKeyMap);
            message.setProperty("dataChange", jsonObject.toJSONString());
            if (delayTime > 0) {
                message.setDelayTime(delayTime, TimeUnit.SECONDS);
            }
            provider.sendMessage(message);
        }
    }
}

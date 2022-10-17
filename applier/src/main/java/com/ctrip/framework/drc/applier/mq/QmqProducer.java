package com.ctrip.framework.drc.applier.mq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import muise.ctrip.canal.DataChange;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.dal.DalTransactionProvider;
import qunar.tc.qmq.producer.MessageProducerProvider;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public class QmqProducer extends AbstractProducer {

    private MessageProducerProvider provider;

    private final boolean persist;

    public QmqProducer(boolean persist, String realTitanKey) {
        this.persist = persist;
        init(persist, realTitanKey);
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

        String topic = "";
        for (EventData eventData: eventDatas) {
            DataChange dataChange = transfer(eventData);
            Message message = provider.generateMessage(topic);
            JSONObject jsonObject = JSON.parseObject(dataChange.toString());
            message.setProperty("dataChange", jsonObject.toJSONString());
            provider.sendMessage(message);
        }
    }
}

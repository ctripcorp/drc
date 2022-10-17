package com.ctrip.framework.drc.applier.mq;

import muise.ctrip.canal.DataChange;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public interface IProducer {

    void send(List<EventData> eventDatas);
}

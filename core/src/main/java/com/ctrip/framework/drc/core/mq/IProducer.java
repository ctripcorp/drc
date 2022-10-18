package com.ctrip.framework.drc.core.mq;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public interface IProducer {

    void send(List<EventData> eventDatas);
}

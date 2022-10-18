package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.core.mq.IProducer;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public interface Mq {

    List<IProducer> getProducers(String tableName);
}

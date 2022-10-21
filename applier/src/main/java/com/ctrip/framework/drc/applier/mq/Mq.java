package com.ctrip.framework.drc.applier.mq;

import com.ctrip.framework.drc.core.mq.Producer;

import java.util.List;

/**
 * Created by jixinwang on 2022/10/17
 */
public interface Mq {

    List<Producer> getProducers(String tableName);
}

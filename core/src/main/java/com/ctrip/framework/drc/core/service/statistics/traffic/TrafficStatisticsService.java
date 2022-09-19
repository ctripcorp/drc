package com.ctrip.framework.drc.core.service.statistics.traffic;

import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * Created by jixinwang on 2022/9/11
 */
public interface TrafficStatisticsService extends Ordered {

    void send(KafKaTrafficMetric metric);

    void send(CatTrafficMetric metric);
}

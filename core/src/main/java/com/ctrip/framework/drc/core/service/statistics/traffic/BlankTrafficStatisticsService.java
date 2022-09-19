package com.ctrip.framework.drc.core.service.statistics.traffic;

/**
 * Created by jixinwang on 2022/9/11
 */
public class BlankTrafficStatisticsService implements TrafficStatisticsService {
    @Override
    public void send(KafKaTrafficMetric metric) {

    }

    @Override
    public void send(CatTrafficMetric metric) {

    }

    @Override
    public int getOrder() {
        return 1;
    }
}

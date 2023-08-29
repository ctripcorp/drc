package com.ctrip.framework.drc.core.service.statistics.traffic;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * Created by jixinwang on 2022/9/9
 */
public class HickWallMessengerDelayEntity {

    Metric metric;

    List<List<Object>> values;
    
    
    public static class Metric{
        String mhaName;
    }
    public String getMha(){
        if(metric == null){
            return null;
        }
        return metric.mhaName;
    }

    public Long getDelay() {
        String delay = (String) values.get(values.size() - 1).get(1);
        BigDecimal bd = new BigDecimal(delay);
        return bd.setScale(0, RoundingMode.UP).longValue();
    }
}

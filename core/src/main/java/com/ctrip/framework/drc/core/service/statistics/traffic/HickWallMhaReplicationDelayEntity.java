package com.ctrip.framework.drc.core.service.statistics.traffic;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * @ClassName HickWallDrcDelayEntity
 * @Author haodongPan
 * @Date 2023/9/7 14:35
 * @Version: $
 */
public class HickWallMhaReplicationDelayEntity {

    Metric metric;

    List<List<Object>> values;


    public static class Metric {
        String srcMha;
        String destMha;
        String role;
        String address;
        String srcDc;
        String destDc;
    }

    public String getSrcMha() {
        if(metric == null){
            return null;
        }
        return metric.srcMha;
    }

    public String getDestMha() {
        if(metric == null){
            return null;
        }
        return metric.destMha;
    }

    public String getRole() {
        if(metric == null){
            return null;
        }
        return metric.role;
    }


    public String getAddress() {
        if(metric == null){
            return null;
        }
        return metric.address;
    }
    
    public String getSrcDc() {
        if(metric == null){
            return null;
        }
        return metric.srcDc;
    }
    
    public String getDestDc() {
        if(metric == null){
            return null;
        }
        return metric.destDc;
    }
    
    public Long getDelay() { 
        String delay = (String) values.get(values.size() - 1).get(1);
        BigDecimal bd = new BigDecimal(delay);
        return bd.setScale(0, RoundingMode.UP).longValue();
    }
}

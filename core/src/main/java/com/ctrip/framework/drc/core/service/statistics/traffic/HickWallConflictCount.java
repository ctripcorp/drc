package com.ctrip.framework.drc.core.service.statistics.traffic;

import java.util.List;

/**
 * @ClassName HickWallConflictRowsCount
 * @Author haodongPan
 * @Date 2023/11/22 14:35
 * @Version: $
 */
public class HickWallConflictCount {
    Metric metric;

    List<List<Object>> values;

    public static class Metric {
        String srcMha;
        String destMha;
        String db;
        String table;
    }
    
    public Long getCount() {
        // get latest value
        String count = (String) values.get(values.size() - 1).get(1);
        return Long.parseLong(count);
    }
    
    public String getSrcMha() {
        return metric.srcMha;
    }
    
    public String getDestMha() {
        return metric.destMha;
    }
    
    public String getDb() {
        return metric.db;
    }
    
    public String getTable() {
        return metric.table;
    }
    
}

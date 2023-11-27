package com.ctrip.framework.drc.core.service.statistics.traffic;

import java.util.List;
import org.apache.commons.lang3.tuple.Triple;

/**
 * @ClassName HickWallConflictRowsCount
 * @Author haodongPan
 * @Date 2023/11/22 14:35
 * @Version: $
 */
public class HickWallConflictRowCount {
    Metric metric;

    List<List<Object>> values;

    public static class Metric {
        String srcMha;
        String destMha;
        String db;
        String table;
    }
    
    public Long getRowCount() {
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
    
    public Triple<String,String,Long> getDbTableCflCount() {
        return Triple.of(metric.db, metric.table, getRowCount());
    }
}

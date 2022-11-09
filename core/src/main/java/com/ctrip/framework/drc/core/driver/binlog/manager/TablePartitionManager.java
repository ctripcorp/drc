package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/11/9
 */
public class TablePartitionManager {

    public static final List<String> ALTER_PARTITION_MANAGEMENT = Lists.newArrayList(
            "(?i)[\\s\\S]*REBUILD[\\s]*PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*OPTIMIZE[\\s]*PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*ANALYZE[\\s]*PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*REPAIR[\\s]*PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*CHECK[\\s]*PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*TRUNCATE[\\s]*PARTITION[\\s\\S]*");

    public static final List<String> CREATE_PARTITION_MANAGEMENT = Lists.newArrayList(
            "(?i)PARTITION[\\s]*BY[\\s\\S]*\\)",
            "(?i)PARTITIONS[\\s]*[1-9][0-9]*");

    public static boolean transformAlterPartition(String queryString) {
        queryString = queryString.toUpperCase();
        for (String action : ALTER_PARTITION_MANAGEMENT) {
            if (queryString.matches(action)) {
                return true;
            }
        }
        return false;
    }

    public static Pair<Boolean, String> transformCreatePartition(String queryString) {
        boolean transformed = false;
        String newQueryString = queryString;
        for (String action : CREATE_PARTITION_MANAGEMENT) {
            newQueryString = newQueryString.replaceFirst(action, "");
            if (!newQueryString.equalsIgnoreCase(queryString) && !transformed) {
                transformed = true;
            }
        }
        return Pair.from(transformed, newQueryString);
    }
}

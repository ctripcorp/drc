package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/11/9
 */
public class TablePartitionManager {

    // https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
    public static final List<String> ALTER_PARTITION_MANAGEMENT = Lists.newArrayList(
            "(?i)[\\s\\S]*ADD[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*DROP[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*DISCARD[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*IMPORT[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*TRUNCATE[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*COALESCE[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*REORGANIZE[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*EXCHANGE[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*ANALYZE[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*CHECK[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*OPTIMIZE[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*REBUILD[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*REPAIR[\\s]+PARTITION[\\s\\S]*",
            "(?i)[\\s\\S]*REMOVE[\\s]+PARTITIONING[\\s\\S]*",
            "(?i)[\\s\\S]*UPGRADE[\\s]+PARTITIONING[\\s\\S]*");

    // https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    public static final List<String> CREATE_PARTITION_MANAGEMENT = Lists.newArrayList(
            "(?i)PARTITION[\\s]+BY[\\s\\S]*\\)",
            "(?i)SUBPARTITIONS[\\s]+[1-9][0-9]*",
            "(?i)PARTITIONS[\\s]+[1-9][0-9]*");

    public static final List<String> COMMENT_MANAGEMENT = Lists.newArrayList(
            "(?i)/\\*[\\s\\S]*\\*/",
            "\n");

    public static boolean transformAlterPartition(String queryString) {
        if (!DynamicConfig.getInstance().getTablePartitionSwitch()) {
            return false;
        }
        queryString = queryString.toUpperCase();
        for (String action : ALTER_PARTITION_MANAGEMENT) {
            if (queryString.matches(action)) {
                return true;
            }
        }
        return false;
    }

    public static Pair<Boolean, String> transformCreatePartition(String queryString) {
        return doTransform(queryString, CREATE_PARTITION_MANAGEMENT);
    }

    public static Pair<Boolean, String> transformComment(String queryString) {
        return doTransform(queryString, COMMENT_MANAGEMENT);
    }

    public static Pair<Boolean, String> doTransform(String queryString, List<String> patterns) {
        if (!DynamicConfig.getInstance().getTablePartitionSwitch()) {
            return Pair.from(false, queryString);
        }
        boolean transformed = false;
        String newQueryString = queryString;
        for (String action : patterns) {
            newQueryString = newQueryString.replaceAll(action, "");
            if (!newQueryString.equalsIgnoreCase(queryString) && !transformed) {
                transformed = true;
            }
        }
        return Pair.from(transformed, newQueryString);
    }
}

package com.ctrip.framework.drc.console.utils;

import java.util.regex.Pattern;

public class DalclusterUtils {
    private static final Pattern pattern = Pattern.compile("shard\\d+db");

    public static String getDalClusterName(String dbName) {
        if (pattern.matcher(dbName).find()) {
            return dbName.replaceAll("shard\\d+db", "shardbasedb_dalcluster");
        } else {
            return dbName + "_dalcluster";
        }
    }
}

package com.ctrip.framework.drc.console.common;

import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.springframework.util.StopWatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/10 21:31
 */
public class AviatorRegexFilterTest {

    @Test
    public void testFilter() {
        String filter = "fncbicardindexdb\\.(uid_brandid_collectionid_rel_\\d*)";
        AviatorRegexFilter aviatorRegexFilter = new AviatorRegexFilter(filter);
        System.out.println(aviatorRegexFilter.filter("fncbicardindexdb.uid_brandid_collectionid_rel_ss"));
    }

    @Test
    public void testString() {
        List<String> list = Lists.newArrayList("a", "b", "c");
        System.out.println(Joiner.on("|").join(list));
    }

    @Test
    public void testDbBlacklist() {
        StringBuilder tableName = new StringBuilder("(table");
        for (int i = 1; i <= 20; i++) {
            tableName.append("|table" + i);
        }
        tableName.append(")");
        List<String> blacklist = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            blacklist.add("db" + i + "\\." + tableName);
        }

        String fullName = "db200.table20";

        long startTime = System.currentTimeMillis();
        for (int i = 0; i<= 10000; i++) {
            AviatorRegexFilter filter = new AviatorRegexFilter(Joiner.on(",").join(blacklist));
            filter.filter(fullName);
        }
        long endTime1 = System.currentTimeMillis();

        for (int i = 0; i<= 10000; i++) {
            filter(fullName, blacklist);
        }

        long endTime2 = System.currentTimeMillis();

        System.out.println(" cost: " + (endTime1 - startTime));
        System.out.println(" cost: " + (endTime2 - endTime1));
    }

    @Test
    public void testDbBlacklist1() {
        StringBuilder tableName = new StringBuilder("(table");
        for (int i = 1; i <= 200; i++) {
            tableName.append("|table" + i);
        }
        tableName.append(")");
        List<String> blacklist = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            blacklist.add("db" + i + "\\." + tableName);
        }

        String fullName = "db2.table190";

        long startTime = System.currentTimeMillis();
        boolean filter = filter(fullName, blacklist);
        long endTime = System.currentTimeMillis();

        long startTime1 = System.currentTimeMillis();
        boolean filter1 = filter1(fullName, blacklist);
        long endTime1 = System.currentTimeMillis();

        System.out.println("res: " + filter + " cost: " + (endTime - startTime));
        System.out.println("res1: " + filter1 + " cost: " + (endTime1 - startTime1));
    }

    private boolean filter(String fullName, List<String> blacklist) {
        int size = blacklist.size();
        List<String> filterBlacklist = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            filterBlacklist.add(blacklist.get(i));
            if (i % 5 == 0 || i == size - 1) {
                AviatorRegexFilter filter = new AviatorRegexFilter(Joiner.on(",").join(filterBlacklist));
                if (filter.filter(fullName)) {
                    return true;
                }
                filterBlacklist = new ArrayList<>();
            }
        }
        return false;
    }

    private boolean filter1(String fullName, List<String> blacklist) {
        int size = blacklist.size();
        for (int i = 0; i < size; i++) {
            AviatorRegexFilter filter = new AviatorRegexFilter(blacklist.get(i));
            if (filter.filter(fullName)) {
                return true;
            }
        }
        return false;
    }
}

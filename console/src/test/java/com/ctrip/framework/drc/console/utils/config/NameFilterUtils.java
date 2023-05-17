package com.ctrip.framework.drc.console.utils.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName NameFilterUtils
 * @Author haodongPan
 * @Date 2023/4/19 14:10
 * @Version: $
 */
public class NameFilterUtils {

    private static final Logger logger = LoggerFactory.getLogger(NameFilterUtils.class);


    @Test
    public void distinctAndUnion(){
        // input dbName & tableNames &(commonPrefix) check and get nameFilter
        String dbName = "CarSDVendorDB";
        String tableNames = "osd_vendor_store_mapping,platform_vendor_mapping";
        String commonPrefix = "";

        List<String> tables = removeBlankAndSplit(tableNames);
        distinctAndCount(tables);

//        String nameFilter = combine(dbName, tables);
        String nameFilter = combine(dbName,tables,commonPrefix);

        logger.info("nameFilter as follows: \n{}",nameFilter);
    }


    private List<String> removeBlankAndSplit(String tableNames) {
        tableNames = tableNames.replaceAll(" ", "");
        String[] tablesArray = tableNames.split(",");
        return Lists.newArrayList(tablesArray);
    }

    private void distinctAndCount(List<String> tables){
        HashMap<String, Integer> tableCountMap = Maps.newHashMap();
        Iterator<String> iterator = tables.iterator();
        while (iterator.hasNext()) {
            String table = iterator.next();
            Integer count = tableCountMap.get(table);
            if (count == null) {
                tableCountMap.put(table,1);
            } else {
                logger.error("RepeatTable: {}, repeatCount: {}",table,count);
                iterator.remove();
                tableCountMap.put(table,++count);
            }
        }
        logger.info("count after distinct, size: {}",tables.size());
    }


    private String combine(String dbName,List<String> tables) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            if (i == 0) {
                builder.append("(");
            } else {
                builder.append("|");
            }
            builder.append(tables.get(i));
            if (i == tables.size() -1) {
                builder.append(")");
            }
        }
        return  dbName + "\\." + builder;
    }

    private String combine(String dbName,List<String> tables,String commonPrefix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            if (i == 0) {
                builder.append(commonPrefix).append("(");
            } else {
                builder.append("|");
            }
            String table = tables.get(i);
            if (!table.startsWith(commonPrefix)) {
                throw new IllegalArgumentException(table + " notStartWith " + commonPrefix);
            }
            builder.append(table.substring(commonPrefix.length()));
            if (i == tables.size() -1) {
                builder.append(")");
            }
        }
        return  dbName + "\\." + builder;
    }
}

package com.ctrip.framework.drc.console.service.v2;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/6/8 14:14
 */
public class NameFilterTest {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String MONITOR_DB = "drcmonitordb\\.delaymonitor";

    @Test
    public void testSplitNameFilter() {
        String nameFilter = "drcmonitordb\\.delaymonitor,bbzcredentialshard[0][1-8]db\\.(sjp_credential_keyid_[1-8]|order_associate),bbzudlshard0[1-8]db\\.udl_info_[1-8]";
        List<String> splitDbs = Lists.newArrayList(nameFilter.split(","));
        if (splitDbs.size() <= 1 || !splitDbs.get(0).equals(MONITOR_DB)) {
            logger.error("split nameFilter error");
        }
        splitDbs.remove(MONITOR_DB);
        List<String> dbs = splitDbs.stream().map(db -> splitNameFilter(db)).collect(Collectors.toList());
        String newNameFilter = StringUtils.join(dbs, ",");
        dbs.forEach(db -> {
            String[] dbString = db.split(",");
            for (String str : dbString) {
                System.out.println(str);
            }

        });
        System.out.println(newNameFilter);
    }


    private String splitNameFilter(String db) {
        if (!db.contains("(")) {
            return db;
        }
        String[] dbStrings = db.split("\\.");
        String dbName = dbStrings[0];
        String[] preTableStr = dbStrings[1].split("\\(");
        String prefixTable = preTableStr[0];

        String[] sufTableStr = preTableStr[1].split("\\)");
        String tableFilter = sufTableStr[0];

        String suffixTable = "";
        if (sufTableStr.length == 2) {
            suffixTable = sufTableStr[1];
        }
        String finalSuffixTable = suffixTable;

        String[] tables = tableFilter.split("\\|");
        List<String> dbFilters = Arrays.stream(tables).map(table -> buildNameFilter(dbName, table, prefixTable, finalSuffixTable)).collect(Collectors.toList());
        String newNameFilter = StringUtils.join(dbFilters, ",");
        return newNameFilter;
    }


    private String buildNameFilter(String dbName, String table, String prefixTable, String suffixTable) {
        return dbName + "." + prefixTable + table + suffixTable;
    }

}

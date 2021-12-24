package com.ctrip.framework.drc.console.monitor.consistency.table;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/12/27 下午3:00.
 */
public class DefaultTableProvider implements TableProvider {

    private List<String> tables;

    private int currentIndex = -1;

    public DefaultTableProvider(String tables) {
        String[] tableArray = StringUtils.split(tables,",");
        this.tables = Lists.newArrayList(tableArray);
    }

    @Override
    public synchronized String next() {

        ++currentIndex;
        if (currentIndex == tables.size()) {
            currentIndex = 0;
        }

        return tables.get(currentIndex);
    }
}

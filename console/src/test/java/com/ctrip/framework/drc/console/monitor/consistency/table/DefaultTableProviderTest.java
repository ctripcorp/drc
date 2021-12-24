package com.ctrip.framework.drc.console.monitor.consistency.table;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mingdongli
 * 2019/12/27 下午3:12.
 */
public class DefaultTableProviderTest {

    private static final String table1 = "db1.table1";

    private static final String table2 = "db2.table2";

    private static final String table3 = "db3.table3";

    private static final String table4 = "db4.table4";

    private static final String tables = table1 + "," + table2 + "," + table3 + "," + table4;

    @Test
    public void next() {

        TableProvider tableProvider = new DefaultTableProvider(tables);
        String table = tableProvider.next();
        Assert.assertEquals(table1, table);

        table = tableProvider.next();
        Assert.assertEquals(table2, table);

        table = tableProvider.next();
        Assert.assertEquals(table3, table);

        table = tableProvider.next();
        Assert.assertEquals(table4, table);

        table = tableProvider.next();
        Assert.assertEquals(table1, table);
    }
}
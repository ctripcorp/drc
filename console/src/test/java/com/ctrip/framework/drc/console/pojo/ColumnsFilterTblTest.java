package com.ctrip.framework.drc.console.pojo;

import com.ctrip.framework.drc.console.dao.entity.v2.ColumnsFilterTblV2;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/6/25 16:27
 */
public class ColumnsFilterTblTest {

    @Test
    public void testEquals() {
        Set<ColumnsFilterTblV2> columns = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            ColumnsFilterTblV2 tbl = new ColumnsFilterTblV2();
            tbl.setMode(i);
            tbl.setColumns("column" + i);
            tbl.setDeleted(0);
            columns.add(tbl);
        }

        ColumnsFilterTblV2 tbl = new ColumnsFilterTblV2();
        tbl.setMode(1);
        tbl.setColumns("column1");
        tbl.setDeleted(1);
        columns.add(tbl);

        columns.forEach(System.out::println);
        Assert.assertEquals(columns.size(), 5);
    }
}

package com.ctrip.framework.drc.console.pojo;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by dengquanliang
 * 2023/12/6 14:56
 */
public class CreatePojoTest {

    @Test
    public void testCreateDdlHistoryPojo() {
        DdlHistoryTbl tbl = DdlHistoryTbl.createDdlHistoryPojo(1L, "ddl", 0, "db", "table");
        Assert.assertEquals("ddl", tbl.getDdl());
        Assert.assertEquals("db", tbl.getSchemaName());
        Assert.assertEquals("table", tbl.getTableName());
    }

    @Test
    public void testCreateBuPojo() {
        BuTbl tbl = BuTbl.createBuPojo("bu");
        Assert.assertEquals("bu", tbl.getBuName());
    }

    @Test
    public void testCreateDcPojo() {
        DcTbl dcTbl = DcTbl.createDcPojo("dc");
        Assert.assertEquals("dc", dcTbl.getDcName());
    }

    @Test
    public void testCreateRoutePojo() {
        RouteTbl tbl = RouteTbl.createRoutePojo(1L, 1L, 1L,
                "srcProxyIds", "relayProxyIds", "dstProxyIds", "tag");
        Assert.assertTrue(tbl.getRouteOrgId() == 1L);
        Assert.assertTrue(tbl.getSrcDcId() == 1L);
        Assert.assertTrue(tbl.getDstDcId() == 1L);
        Assert.assertEquals("srcProxyIds", tbl.getSrcProxyIds());
        Assert.assertEquals("relayProxyIds", tbl.getOptionalProxyIds());
        Assert.assertEquals("dstProxyIds", tbl.getDstProxyIds());
        Assert.assertEquals("tag", tbl.getTag());

    }
}

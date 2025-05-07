package com.ctrip.framework.drc.console.vo.api;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author yongnian
 * @create 2025/4/27 14:52
 */
public class DbTableDrcRegionInfoTest {

    @Test
    public void testEquals() {
        RegionInfo regionInfo1 = new RegionInfo("src", "dst");
        RegionInfo regionInfo2 = new RegionInfo("src", "dst");
        Assert.assertEquals(regionInfo1, regionInfo2);

        DbTableDrcRegionInfo dbTableDrcRegionInfo1 = new DbTableDrcRegionInfo("db", "table", List.of(regionInfo1));
        DbTableDrcRegionInfo dbTableDrcRegionInfo2 = new DbTableDrcRegionInfo("db", "table", List.of(regionInfo2));
        Assert.assertEquals(dbTableDrcRegionInfo1, dbTableDrcRegionInfo2);
    }

    @Test
    public void testHasDrc() {
        DbTableDrcRegionInfo dbTableDrcRegionInfo = new DbTableDrcRegionInfo("db", "table", List.of(new RegionInfo("src", "dst")));
        SingleDbTableDrcCheckResponse singleDbTableDrcCheckResponse = new SingleDbTableDrcCheckResponse(dbTableDrcRegionInfo);
        Assert.assertTrue(singleDbTableDrcCheckResponse.isHasDrc());
    }

    @Test
    public void testHasNoDrc() {
        DbTableDrcRegionInfo dbTableDrcRegionInfo = new DbTableDrcRegionInfo("db", "table", List.of());
        SingleDbTableDrcCheckResponse singleDbTableDrcCheckResponse = new SingleDbTableDrcCheckResponse(dbTableDrcRegionInfo);
        Assert.assertFalse(singleDbTableDrcCheckResponse.isHasDrc());
    }

    @Test(expected = NullPointerException.class)
    public void testNotNull() {
        new SingleDbTableDrcCheckResponse(null);
    }
}


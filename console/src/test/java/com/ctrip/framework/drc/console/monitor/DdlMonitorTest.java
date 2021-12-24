package com.ctrip.framework.drc.console.monitor;

import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.service.impl.MetaInfoServiceImpl;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyString;

public class DdlMonitorTest {
    @InjectMocks
    private DdlMonitor ddlMonitor;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private MetaGenerator metaService;

    private DdlHistoryTbl ddlHistoryTbl1;
    private DdlHistoryTbl ddlHistoryTbl2;
    private DdlHistoryTbl ddlHistoryTbl3;
    private DdlHistoryTbl ddlHistoryTbl4;
    private DdlHistoryTbl ddlHistoryTbl5;
    private DdlHistoryTbl ddlHistoryTbl6;
    private DdlHistoryTbl ddlHistoryTbl7;
    private MhaTbl mhaTbl1;
    private MhaTbl mhaTbl2;
    private MhaTbl mhaTbl3;
    private MhaTbl mhaTbl4;
    private DdlMonitor.DdlMonitorItem ddlMonitorItem1;
    private DdlMonitor.DdlMonitorItem ddlMonitorItem2;
    private DdlMonitor.DdlMonitorItem ddlMonitorItem3;

    public static final long MHA_ID1 = 1L;
    public static final long MHA_ID2 = 2L;
    public static final long MHA_ID3 = 3L;
    public static final long MHA_ID4 = 4L;
    public static final String CLUSTER = "integration-test";
    public static final String MHA1 = "fat-fx-drc1";
    public static final String MHA2 = "fat-fx-drc2";
    public static final String MHA3 = "drcTestW1";
    public static final String MHA4 = "drcTestW2";
    public static final String SCHEMA1 = "schema1";
    public static final String SCHEMA2 = "schema2";
    public static final String SCHEMA3 = "schema3";
    public static final String TABLE1 = "table1";
    public static final String TABLE2 = "table2";
    public static final String TABLE3 = "table3";
    public static final int TRUNCATE_QUERY_TYPE = 4;
    public static final long MHA_GROUP_ID1 = 1L;
    public static final long MHA_GROUP_ID2 = 2L;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);

        ddlHistoryTbl1 = new DdlHistoryTbl();
        ddlHistoryTbl1.setId(1L);
        ddlHistoryTbl1.setMhaId(MHA_ID1);
        ddlHistoryTbl1.setDdl("truncate schema1.table1");
        ddlHistoryTbl1.setSchemaName(SCHEMA1);
        ddlHistoryTbl1.setTableName(TABLE1);
        ddlHistoryTbl1.setQueryType(TRUNCATE_QUERY_TYPE);

        ddlHistoryTbl2 = new DdlHistoryTbl();
        ddlHistoryTbl2.setId(2L);
        ddlHistoryTbl2.setMhaId(MHA_ID2);
        ddlHistoryTbl2.setDdl("truncate schema1.table1");
        ddlHistoryTbl2.setSchemaName(SCHEMA1);
        ddlHistoryTbl2.setTableName(TABLE1);
        ddlHistoryTbl2.setQueryType(TRUNCATE_QUERY_TYPE);

        ddlHistoryTbl3 = new DdlHistoryTbl();
        ddlHistoryTbl3.setId(3L);
        ddlHistoryTbl3.setMhaId(MHA_ID1);
        ddlHistoryTbl3.setDdl("truncate schema2.table2");
        ddlHistoryTbl3.setSchemaName(SCHEMA2);
        ddlHistoryTbl3.setTableName(TABLE2);
        ddlHistoryTbl3.setQueryType(TRUNCATE_QUERY_TYPE);

        ddlHistoryTbl4 = new DdlHistoryTbl();
        ddlHistoryTbl4.setId(4L);
        ddlHistoryTbl4.setMhaId(MHA_ID2);
        ddlHistoryTbl4.setDdl("truncate schema2.table2");
        ddlHistoryTbl4.setSchemaName(SCHEMA2);
        ddlHistoryTbl4.setTableName(TABLE2);
        ddlHistoryTbl4.setQueryType(TRUNCATE_QUERY_TYPE);

        ddlHistoryTbl5 = new DdlHistoryTbl();
        ddlHistoryTbl5.setId(5L);
        ddlHistoryTbl5.setMhaId(MHA_ID3);
        ddlHistoryTbl5.setDdl("truncate schema3.table3");
        ddlHistoryTbl5.setSchemaName(SCHEMA3);
        ddlHistoryTbl5.setTableName(TABLE3);
        ddlHistoryTbl5.setQueryType(TRUNCATE_QUERY_TYPE);

        ddlHistoryTbl6 = new DdlHistoryTbl();
        ddlHistoryTbl6.setId(6L);
        ddlHistoryTbl6.setMhaId(MHA_ID4);
        ddlHistoryTbl6.setDdl("truncate schema3.table3");
        ddlHistoryTbl6.setSchemaName(SCHEMA3);
        ddlHistoryTbl6.setTableName(TABLE3);
        ddlHistoryTbl6.setQueryType(TRUNCATE_QUERY_TYPE);

        ddlHistoryTbl7 = new DdlHistoryTbl();
        ddlHistoryTbl7.setId(7L);
        ddlHistoryTbl7.setMhaId(MHA_ID4);
        ddlHistoryTbl7.setDdl("truncate schema3.table3");
        ddlHistoryTbl7.setSchemaName(SCHEMA3);
        ddlHistoryTbl7.setTableName(TABLE3);
        ddlHistoryTbl7.setQueryType(TRUNCATE_QUERY_TYPE);

        mhaTbl1 = new MhaTbl();
        mhaTbl1.setId(MHA_ID1);
        mhaTbl1.setMhaName(MHA1);
        mhaTbl1.setMhaGroupId(MHA_GROUP_ID1);

        mhaTbl2 = new MhaTbl();
        mhaTbl2.setId(MHA_ID2);
        mhaTbl2.setMhaName(MHA2);
        mhaTbl2.setMhaGroupId(MHA_GROUP_ID1);

        mhaTbl3 = new MhaTbl();
        mhaTbl3.setId(MHA_ID3);
        mhaTbl3.setMhaName(MHA3);
        mhaTbl3.setMhaGroupId(MHA_GROUP_ID2);

        mhaTbl4 = new MhaTbl();
        mhaTbl4.setId(MHA_ID4);
        mhaTbl4.setMhaName(MHA4);
        mhaTbl4.setMhaGroupId(MHA_GROUP_ID2);

        Mockito.when(metaInfoService.getMhaGroupId(MHA_ID1)).thenReturn(MHA_GROUP_ID1);
        Mockito.when(metaInfoService.getMhaGroupId(MHA_ID2)).thenReturn(MHA_GROUP_ID1);
        Mockito.when(metaInfoService.getMhaGroupId(MHA_ID3)).thenReturn(MHA_GROUP_ID2);
        Mockito.when(metaInfoService.getMhaGroupId(MHA_ID4)).thenReturn(MHA_GROUP_ID2);
        Mockito.when(metaInfoService.getMhaTbls(MHA_GROUP_ID1)).thenReturn(Arrays.asList(mhaTbl1, mhaTbl2));
        Mockito.when(metaInfoService.getMhaTbls(MHA_GROUP_ID2)).thenReturn(Arrays.asList(mhaTbl3, mhaTbl4));
        Mockito.when(metaInfoService.getCluster(anyString())).thenReturn(CLUSTER);

        ddlMonitorItem1 = new DdlMonitor.DdlMonitorItem(MHA_GROUP_ID1, SCHEMA1, TABLE1, CLUSTER, Arrays.asList(MHA1, MHA2), TRUNCATE_QUERY_TYPE);
        ddlMonitorItem2 = new DdlMonitor.DdlMonitorItem(MHA_GROUP_ID1, SCHEMA2, TABLE2, CLUSTER, Arrays.asList(MHA1, MHA2), TRUNCATE_QUERY_TYPE);
        ddlMonitorItem3 = new DdlMonitor.DdlMonitorItem(MHA_GROUP_ID2, SCHEMA3, TABLE3, CLUSTER, Arrays.asList(MHA3, MHA4), TRUNCATE_QUERY_TYPE);
    }

    @Test
    public void testBuildTruncateHistoryMap() throws SQLException {
        Mockito.when(metaService.getDdlHistoryTbls()).thenReturn(Arrays.asList(ddlHistoryTbl1, ddlHistoryTbl2, ddlHistoryTbl3, ddlHistoryTbl5, ddlHistoryTbl6, ddlHistoryTbl7));
        Mockito.when(metaService.getMhaTbls()).thenReturn(Arrays.asList(mhaTbl1, mhaTbl2, mhaTbl3, mhaTbl4));

        Map<DdlMonitor.DdlMonitorItem, Map<String, AtomicInteger>> allItemTruncateHistory = ddlMonitor.buildTruncateHistoryMap();
        Assert.assertEquals(3, allItemTruncateHistory.size());
        Assert.assertTrue(allItemTruncateHistory.containsKey(ddlMonitorItem1));
        Assert.assertTrue(allItemTruncateHistory.containsKey(ddlMonitorItem2));
        Assert.assertTrue(allItemTruncateHistory.containsKey(ddlMonitorItem3));

        Map<String, AtomicInteger> truncateCountForItem1 = allItemTruncateHistory.get(ddlMonitorItem1);
        Map<String, AtomicInteger> truncateCountForItem2 = allItemTruncateHistory.get(ddlMonitorItem2);
        Map<String, AtomicInteger> truncateCountForItem3 = allItemTruncateHistory.get(ddlMonitorItem3);
        Assert.assertEquals(2, truncateCountForItem1.size());
        Assert.assertEquals(1, truncateCountForItem2.size());
        Assert.assertEquals(2, truncateCountForItem3.size());
        Assert.assertTrue(truncateCountForItem1.containsKey(MHA1));
        Assert.assertTrue(truncateCountForItem1.containsKey(MHA2));
        Assert.assertTrue(truncateCountForItem2.containsKey(MHA1));
        Assert.assertTrue(truncateCountForItem3.containsKey(MHA3));
        Assert.assertTrue(truncateCountForItem3.containsKey(MHA4));
        Assert.assertEquals(1, truncateCountForItem1.get(MHA1).get());
        Assert.assertEquals(1, truncateCountForItem1.get(MHA2).get());
        Assert.assertEquals(1, truncateCountForItem2.get(MHA1).get());
        Assert.assertEquals(1, truncateCountForItem3.get(MHA3).get());
        Assert.assertEquals(2, truncateCountForItem3.get(MHA4).get());
    }

    @Test
    public void testCheckTruncateInconsistency() throws SQLException {
        Mockito.when(metaService.getDdlHistoryTbls()).thenReturn(Arrays.asList(ddlHistoryTbl1, ddlHistoryTbl2, ddlHistoryTbl3, ddlHistoryTbl5, ddlHistoryTbl6, ddlHistoryTbl7));
        Mockito.when(metaService.getMhaTbls()).thenReturn(Arrays.asList(mhaTbl1, mhaTbl2, mhaTbl3, mhaTbl4));

        Map<DdlMonitor.DdlMonitorItem, Map<String, AtomicInteger>> allItemTruncateHistory = ddlMonitor.buildTruncateHistoryMap();
        Set<DdlMonitor.DdlMonitorItem> inconsistentItem = ddlMonitor.checkTruncateInconsistency(allItemTruncateHistory);
        Assert.assertEquals(2, inconsistentItem.size());
        Assert.assertTrue(inconsistentItem.contains(ddlMonitorItem2));
        Assert.assertTrue(inconsistentItem.contains(ddlMonitorItem3));
    }

    @Test
    public void testCompareAndAlertInconsistenctTruncate() {
        Set<DdlMonitor.DdlMonitorItem> inconsistentItem = new HashSet<>(Arrays.asList(ddlMonitorItem1, ddlMonitorItem3));

        ddlMonitor.truncateInconsistencyCounter.clear();
        Assert.assertEquals(0, ddlMonitor.compareAndAlertInconsistenctTruncate(inconsistentItem));

        ddlMonitor.truncateInconsistencyCounter.clear();
        ddlMonitor.truncateInconsistencyCounter.put(ddlMonitorItem1, new AtomicInteger(2));
        ddlMonitor.truncateInconsistencyCounter.put(ddlMonitorItem2, new AtomicInteger(2));
        Assert.assertEquals(1, ddlMonitor.compareAndAlertInconsistenctTruncate(inconsistentItem));

        ddlMonitor.truncateInconsistencyCounter.clear();
        ddlMonitor.truncateInconsistencyCounter.put(ddlMonitorItem1, new AtomicInteger(2));
        ddlMonitor.truncateInconsistencyCounter.put(ddlMonitorItem2, new AtomicInteger(3));
        ddlMonitor.truncateInconsistencyCounter.put(ddlMonitorItem3, new AtomicInteger(2));
        Assert.assertEquals(2, ddlMonitor.compareAndAlertInconsistenctTruncate(inconsistentItem));
    }
}

package com.ctrip.framework.drc.console.service.v2.impl.migrate;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.InputStream;

public class MetaCompareServiceImplTest {

    @InjectMocks
    private MetaCompareServiceImpl metaCompareService;

    @Mock
    private MetaProviderV2 metaProviderV2;

    @Mock
    private DbClusterSourceProvider metaProviderV1;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private MysqlServiceV2 mysqlServiceV2;

    private Drc oldDrc;
    private Drc newDrc;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        InputStream ins = FileUtils.getFileInputStream("oldMeta.xml");
        oldDrc = DefaultSaxParser.parse(ins);

        ins = FileUtils.getFileInputStream("newMeta.xml");
        newDrc = DefaultSaxParser.parse(ins);

        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("publicCloudRegion"));
        Mockito.when(consoleConfig.getRegion()).thenReturn("localRegion");

        Mockito.when(metaProviderV1.getDrc()).thenReturn(oldDrc);
        Mockito.when(metaProviderV2.getDrc()).thenReturn(newDrc);


        // for old meta
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha1"), Mockito.eq("db1\\.t2,db2\\.t2")))
                .thenReturn(Lists.newArrayList("db1.t2", "db2.t2"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha3"), Mockito.eq("nameMappingshard0[1-2]db\\.t[1-8]")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.t1", "nameMappingshard02db.t1"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha2"), Mockito.eq("nameMappingshard0[1-2]db\\.s[1-8],rowsFiltersharddb[1-2]\\.s[1-8],columnsFiltershardb[1-2]\\..*")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.s1", "nameMappingshard02db.s1", "rowsFiltersharddb1.s1", "rowsFiltersharddb2.s1", "columnsFiltershardb1.s1", "columnsFiltershardb2.s1"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha2"), Mockito.eq("rowsFiltersharddb[1-2]\\.s[1-8]")))
                .thenReturn(Lists.newArrayList("rowsFiltersharddb1.s1", "rowsFiltersharddb2.s1"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha2"), Mockito.eq("columnsFiltershardb[1-2]\\..*")))
                .thenReturn(Lists.newArrayList("columnsFiltershardb1.s1", "columnsFiltershardb2.s1"));

        // for new meta
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha1"), Mockito.eq("db1\\.t2,db2\\.t2")))
                .thenReturn(Lists.newArrayList("db1.t2", "db2.t2"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha3"), Mockito.eq("nameMappingshard01db\\.t[1-8],nameMappingshard02db\\.t[1-8]")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.t1", "nameMappingshard02db.t1"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha2"), Mockito.eq("nameMappingshard01db\\.s[1-8],nameMappingshard02db\\.s[1-8],rowsFiltersharddb1\\.s[1-8],rowsFiltersharddb2\\.s[1-8],columnsFiltershardb1\\..*,columnsFiltershardb2\\..*")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.s1", "nameMappingshard02db.s1", "rowsFiltersharddb1.s1", "rowsFiltersharddb2.s1", "columnsFiltershardb1.s1", "columnsFiltershardb2.s1"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha2"), Mockito.eq("rowsFiltersharddb1\\.s[1-8],rowsFiltersharddb2\\.s[1-8]")))
                .thenReturn(Lists.newArrayList("rowsFiltersharddb1.s1", "rowsFiltersharddb2.s1"));
        Mockito.when(mysqlServiceV2.queryTablesWithNameFilter(Mockito.eq("mha2"), Mockito.eq("columnsFiltershardb1\\..*,columnsFiltershardb2\\..*")))
                .thenReturn(Lists.newArrayList("columnsFiltershardb1.s1", "columnsFiltershardb2.s1"));


        Mockito.when(consoleConfig.getMetaCompareParallel()).thenReturn(1);
    }

    @Test
    public void testCompareLogically() throws Exception {
        String compareRecorder = metaCompareService.compareDrcMeta();
        boolean equals = !(compareRecorder.contains("not equal") || compareRecorder.contains("empty"));
        Assert.assertTrue(equals);
    }

    @Test
    public void testCompareDbCluster() {
        Mockito.when(metaProviderV1.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(oldDrc.findDc("ntgxy"));
        Mockito.when(metaProviderV2.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(newDrc.findDc("ntgxy"));

        DbClusterCompareRes dbClusterCompareRes = metaCompareService.compareDbCluster("mha3_dalcluster.mha3");
        String compareRes = dbClusterCompareRes.getCompareRes();
        Assert.assertTrue((!compareRes.contains("not equal")) && (!compareRes.contains("empty")) && (!compareRes.contains("fail")));
    }

    @Test
    public void testConsistentInput() {
        Assert.assertTrue(metaCompareService.isConsistent("something normal"));
        Assert.assertFalse(metaCompareService.isConsistent("something, not equal, and..."));
        Assert.assertFalse(metaCompareService.isConsistent("....,empty,...."));
        Assert.assertFalse(metaCompareService.isConsistent(",....fail,...."));
        Assert.assertFalse(metaCompareService.isConsistent("not equal, and..."));
        Assert.assertFalse(metaCompareService.isConsistent("empty,...."));
        Assert.assertFalse(metaCompareService.isConsistent("fail,...."));
    }
}
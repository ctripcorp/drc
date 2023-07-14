package com.ctrip.framework.drc.console.service.v2.impl.migrate;


import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.MhaGrayConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.xml.sax.SAXException;

public class MetaGrayServiceImplTest {

    @InjectMocks private MetaGrayServiceImpl metaGrayService;
    
    @Mock private MetaProviderV2 metaProviderV2;

    @Mock private DbClusterSourceProvider metaProviderV1;

    @Mock private DrcBuildService drcBuildService;

    @Mock private DefaultConsoleConfig consoleConfig;
    
    @Mock private MhaGrayConfig mhaGrayConfig;

    private Drc oldDrc;
    private Drc newDrc;
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        InputStream ins = FileUtils.getFileInputStream("oldMeta.xml");
        oldDrc = DefaultSaxParser.parse(ins);

        ins = FileUtils.getFileInputStream("newMeta.xml");
        newDrc = DefaultSaxParser.parse(ins);

        
        Mockito.when(metaProviderV1.getDrc()).thenReturn(oldDrc);
        Mockito.when(metaProviderV2.getDrc()).thenReturn(newDrc);


        // for old meta
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha1"),Mockito.eq("db1\\.t2,db2\\.t2")))
                .thenReturn(Lists.newArrayList("db1.t2","db2.t2"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha3"),Mockito.eq("nameMappingshard0[1-2]db\\.t[1-8]")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.t1","nameMappingshard02db.t1"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha2"),Mockito.eq("nameMappingshard0[1-2]db\\.s[1-8],rowsFiltersharddb[1-2]\\.s[1-8],columnsFiltershardb[1-2]\\..*")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.s1","nameMappingshard02db.s1","rowsFiltersharddb1.s1","rowsFiltersharddb2.s1","columnsFiltershardb1.s1","columnsFiltershardb2.s1"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha2"),Mockito.eq("rowsFiltersharddb[1-2]\\.s[1-8]")))
                .thenReturn(Lists.newArrayList("rowsFiltersharddb1.s1","rowsFiltersharddb2.s1"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha2"),Mockito.eq("columnsFiltershardb[1-2]\\..*")))
                .thenReturn(Lists.newArrayList("columnsFiltershardb1.s1","columnsFiltershardb2.s1"));
        
        // for new meta
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha1"),Mockito.eq("db1\\.t2,db2\\.t2")))
                .thenReturn(Lists.newArrayList("db1.t2","db2.t2"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha3"),Mockito.eq("nameMappingshard01db\\.t[1-8],nameMappingshard02db\\.t[1-8]")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.t1","nameMappingshard02db.t1"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha2"),Mockito.eq("nameMappingshard01db\\.s[1-8],nameMappingshard02db\\.s[1-8],rowsFiltersharddb1\\.s[1-8],rowsFiltersharddb2\\.s[1-8],columnsFiltershardb1\\..*,columnsFiltershardb2\\..*")))
                .thenReturn(Lists.newArrayList("nameMappingshard01db.s1","nameMappingshard02db.s1","rowsFiltersharddb1.s1","rowsFiltersharddb2.s1","columnsFiltershardb1.s1","columnsFiltershardb2.s1"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha2"),Mockito.eq("rowsFiltersharddb1\\.s[1-8],rowsFiltersharddb2\\.s[1-8]")))
                .thenReturn(Lists.newArrayList("rowsFiltersharddb1.s1","rowsFiltersharddb2.s1"));
        Mockito.when(drcBuildService.queryTablesWithNameFilter(Mockito.eq("mha2"),Mockito.eq("columnsFiltershardb1\\..*,columnsFiltershardb2\\..*")))
                .thenReturn(Lists.newArrayList("columnsFiltershardb1.s1","columnsFiltershardb2.s1"));
        
        
        Mockito.when(consoleConfig.getMetaCompareParallel()).thenReturn(5);
    }

  

    @Test
    public void testCompareLogically() {
        String compareRecorder = metaGrayService.compareDrcMeta();
        boolean equals = !(compareRecorder.contains("not equal") || compareRecorder.contains("empty"));
        Assert.assertTrue(equals);
    }

    @Test
    public void testCompareDbCluster() {
        Mockito.when(metaProviderV1.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(oldDrc.findDc("ntgxy"));
        Mockito.when(metaProviderV2.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(newDrc.findDc("ntgxy"));
        
        DbClusterCompareRes dbClusterCompareRes = metaGrayService.compareDbCluster("mha3_dalcluster.mha3");
        String compareRes = dbClusterCompareRes.getCompareRes();
        Assert.assertTrue((!compareRes.contains("not equal")) && (!compareRes.contains("empty")) && (!compareRes.contains("fail")));
    }

    @Test
    public void testGetDrcInGrayMode() throws IOException, SAXException {
        Mockito.when(mhaGrayConfig.getDbClusterGraySwitch()).thenReturn(true);
        Mockito.when(mhaGrayConfig.getGrayDbClusterSet()).thenReturn(Sets.newHashSet("mha3_dalcluster.mha3"));
        Mockito.when(metaProviderV2.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(newDrc.findDc("ntgxy"));
        Mockito.when(consoleConfig.getRegion()).thenReturn("sha");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("sin"));

        Drc drcGray = metaGrayService.getDrc();
        Drc oldDrc = metaProviderV1.getDrc();
        Assert.assertEquals(oldDrc.findDc("ntgxh"),drcGray.findDc("ntgxh"));
        Assert.assertEquals(newDrc.findDc("ntgxy").findDbCluster("mha3_dalcluster.mha3").toString(),
                drcGray.findDc("ntgxy").findDbCluster("mha3_dalcluster.mha3").toString());
        
        
        Mockito.when(consoleConfig.getRegion()).thenReturn("sin");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("sin"));
        drcGray = metaGrayService.getDrc();
        Assert.assertEquals(oldDrc.toString(),drcGray.toString());
        
        
    }
}
package com.ctrip.framework.drc.console.service.v2.impl.migrate;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.MhaGrayConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.v2.MetaCompareService;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
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

    @Mock private DefaultConsoleConfig consoleConfig;
    
    @Mock private MhaGrayConfig mhaGrayConfig;

    @Mock private MetaCompareService metaCompareService;

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
        Mockito.when(metaCompareService.isConsistent()).thenReturn(true);
        
    }
    
    

    @Test
    public void testGetDrcInGrayMode() throws IOException, SAXException {
        Mockito.when(mhaGrayConfig.getDbClusterGraySwitch()).thenReturn(true);
        Mockito.when(mhaGrayConfig.getDbClusterGrayCompareSwitch()).thenReturn(true);
        Mockito.when(mhaGrayConfig.getGrayDbClusterSet()).thenReturn(Sets.newHashSet("mha3_dalcluster.mha3"));
        Mockito.when(metaProviderV2.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(newDrc.findDc("ntgxy"));
        Mockito.when(consoleConfig.getRegion()).thenReturn("sha");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("sin"));
        Mockito.when(metaProviderV1.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(oldDrc.findDc("ntgxy"));
        Mockito.when(metaProviderV2.getDcBy(Mockito.eq("mha3_dalcluster.mha3"))).thenReturn(newDrc.findDc("ntgxy"));

        Drc drcGray = metaGrayService.getDrc();
        Drc oldDrc = metaProviderV1.getDrc();
        Assert.assertEquals(oldDrc.findDc("ntgxh"),drcGray.findDc("ntgxh"));
        Assert.assertEquals(newDrc.findDc("ntgxy").findDbCluster("mha3_dalcluster.mha3").toString(),
                drcGray.findDc("ntgxy").findDbCluster("mha3_dalcluster.mha3").toString());

        Drc ntgxh = metaGrayService.getDrc("ntgxh");
        Assert.assertEquals(drcGray.findDc("ntgxh").toString(),ntgxh.findDc("ntgxh").toString());

        Mockito.when(consoleConfig.getRegion()).thenReturn("sin");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("sin"));
        metaGrayService.scheduledTask();
        drcGray = metaGrayService.getDrc();
        Assert.assertEquals(oldDrc.toString(),drcGray.toString());
        
        
    }
    
}
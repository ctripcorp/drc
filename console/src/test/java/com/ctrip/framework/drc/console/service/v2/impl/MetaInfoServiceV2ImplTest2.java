package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.service.v2.DataMediaServiceV2;
import com.ctrip.framework.drc.console.utils.XmlUtils;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.SQLException;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.when;

public class MetaInfoServiceV2ImplTest2 extends CommonDataInit {

    @Mock
    DataMediaServiceV2 dataMediaService;

    @Before
    public void setUp() throws SQLException, IOException {
        MockitoAnnotations.openMocks(this);
        super.setUp();
        when(dataMediaService.generateConfigFast(anyList())).thenReturn(new DataMediaConfig());
    }

    @Test
    public void testGetDrcReplicationConfig() throws Exception {

        Drc drc = metaInfoServiceV2Impl.getDrcReplicationConfig(1L);
        String xml = XmlUtils.formatXML(drc.toString());
        System.out.println(xml);

        Assert.assertEquals(2, drc.getDcs().size());
        Dc shaoy = drc.getDcs().get("shaoy");
        Assert.assertNotNull(shaoy);
        DbCluster dbCluster = shaoy.getDbClusters().get("test_dalcluster.mha1");
        Assert.assertNotNull(dbCluster);
        Assert.assertEquals(2, dbCluster.getDbs().getDbs().size());
        Assert.assertEquals(2, dbCluster.getReplicators().size());


        Dc sinaws = drc.getDcs().get("sinaws");
        Assert.assertNotNull(sinaws);
        dbCluster = sinaws.getDbClusters().get("test_dalcluster.mha2");
        Assert.assertNotNull(dbCluster);
        Assert.assertEquals(1, dbCluster.getDbs().getDbs().size());
        Assert.assertEquals(2, dbCluster.getAppliers().size());


    }

    @Test
    public void testGetDrcMessengerConfig() throws Exception {
        Drc result = metaInfoServiceV2Impl.getDrcMessengerConfig("mha1");
    }

}

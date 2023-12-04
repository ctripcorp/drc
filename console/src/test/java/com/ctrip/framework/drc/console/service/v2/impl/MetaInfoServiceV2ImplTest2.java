package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dto.RouteDto;
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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.when;

public class MetaInfoServiceV2ImplTest2 extends CommonDataInit {

    public static String PROXY = "PROXY";
    public static String IP_DC1_1 = "10.25.222.15";
    public static String PORT_IN = "80";

    public static String PROXY_DC1_1 = String.format("%s://%s:%s", PROXY, IP_DC1_1, PORT_IN);

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
    public void testGetDrcReplicationConfigByName() throws Exception {
        Drc drc = metaInfoServiceV2Impl.getDrcReplicationConfig("mha1", "mha2");
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

    @Test
    public void testGetProxyRoutes() throws Exception {
        List<BuTbl> buTbls = this.getData("BuTbl.json", BuTbl.class);
        Mockito.when(buTblDao.queryByBuName(Mockito.anyString())).thenReturn(buTbls.get(0));
        Mockito.when(buTblDao.queryByPk(Mockito.anyLong())).thenReturn(buTbls.get(0));

        List<DcTbl> dcTbls = this.getData("DcTbl.json", DcTbl.class);
        Mockito.when(dcTblDao.queryByDcName(Mockito.eq("shaoy"))).thenReturn(dcTbls.get(0));
        Mockito.when(dcTblDao.queryByDcName(Mockito.eq("sinaws"))).thenReturn(dcTbls.get(1));
        Mockito.when(dcTblDao.queryByPk(Mockito.anyLong())).thenReturn(dcTbls.get(0));

        List<RouteDto> proxyRoutes = metaInfoServiceV2Impl.getRoutes("BBZ", "shaoy", "sinaws", "meta",0);
        Assert.assertEquals(1, proxyRoutes.size());
        for (RouteDto dto : proxyRoutes) {
            Assert.assertEquals("shaoy", dto.getSrcDcName());
            Assert.assertEquals("shaoy", dto.getDstDcName());
            Assert.assertEquals("BBZ", dto.getRouteOrgName());
            Assert.assertTrue("meta".equalsIgnoreCase(dto.getTag()) || "console".equalsIgnoreCase(dto.getTag()));
            List<String> srcProxyIps = dto.getSrcProxyUris();
            List<String> dstProxyIps = dto.getDstProxyUris();
            Assert.assertEquals(1, srcProxyIps.size());
            Assert.assertEquals(0, dstProxyIps.size());
            Assert.assertTrue(srcProxyIps.contains(PROXY_DC1_1));
        }
    }

}

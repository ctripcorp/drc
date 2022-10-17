package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.RowsFilterTblDao;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.dto.MetaProposalDto;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.DrcBuildPreCheckVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.entity.*;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.service.impl.MetaGeneratorTest.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

public class DrcBuildServiceImplTest extends AbstractTest {

    @InjectMocks
    private DrcBuildServiceImpl drcBuildService;

    @Mock
    private MetaInfoServiceImpl metaInfoService;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private DataCenterService dataCenterService;
    
    @Mock
    private RowsFilterService rowsFilterService;

    @InjectMocks
    private MetaGenerator metaService = new MetaGenerator();

    private DalUtils dalUtils = DalUtils.getInstance();

    public static final String MHA1OY = "fat-fx-drc1";

    public static final String MHA1RB = "fat-fx-drc2";

    public static final String MHA2OY = "drcTestW1";

    public static final String MHA2RB = "drcTestW2";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn(DC1).when(dataCenterService).getDc();
        Mockito.when(metaInfoService.getMasterEndpoint(any())).thenReturn(new DefaultEndPoint("10.2.72.230", 55111, "m_drc_w", "80+H44bA5wwqA(!R_"));
        Mockito.when(metaInfoService.findAvailableApplierPort(any())).thenReturn(8888);
        Mockito.when(metaInfoService.getXmlConfiguration(anyLong())).thenReturn("xml");
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("cloudRegion"));
        Mockito.when(consoleConfig.getRegionForDc(Mockito.anyString())).thenReturn("sha");
        Mockito.when(rowsFilterService.generateRowsFiltersConfig(Mockito.anyLong(),Mockito.eq(0))).thenReturn(null);
    }

    @Test
    public void testSubmitConfig() throws Exception {

        MetaProposalDto metaProposalDto = new MetaProposalDto();
        metaProposalDto.setSrcApplierIncludedDbs("drcmonitordb");
        metaProposalDto.setSrcMha(MHA1OY);
        metaProposalDto.setDestMha(MHA1OY);
        String s = drcBuildService.submitConfig(metaProposalDto);
        Assert.assertTrue(s.contains(" are same mha, which is not allowed."));

        metaProposalDto.setDestMha(MHA2RB);
        Mockito.doReturn(null).when(metaInfoService).getMhaGroupId(MHA1OY, MHA2RB);
        s = drcBuildService.submitConfig(metaProposalDto);
        Assert.assertTrue(s.contains(" are NOT in same mha group, cannot establish DRC"));

        Mockito.doReturn(1L).when(metaInfoService).getMhaGroupId(MHA1OY, MHA1RB);
        List<String> expectedReplicatorIps = Arrays.asList("10.2.83.105", "10.2.87.154");
        List<String> expectedApplierIps = Arrays.asList("10.2.86.137", "10.2.86.138");
        metaProposalDto.setDestMha(MHA1RB);
        metaProposalDto.setSrcReplicatorIps(expectedReplicatorIps);
        metaProposalDto.setSrcApplierIps(expectedApplierIps);
        // no change but still need provide the old value
        metaProposalDto.setDestReplicatorIps(Arrays.asList("10.2.83.106", "10.2.86.199"));
        metaProposalDto.setDestApplierIps(Arrays.asList("10.2.86.136", "10.2.86.138"));
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(()-> MySqlUtils.getGtidExecuted(Mockito.any())).thenReturn("gtid");
            s = drcBuildService.submitConfig(metaProposalDto);
            Assert.assertEquals("xml", s);
        }
        String resultXml = metaService.getDrc().toString();
        System.out.println("updated xml: " +  resultXml);
        Drc drc = DefaultSaxParser.parse(resultXml);
        Assert.assertNotNull(drc);
        Dc dc = drc.getDcs().get("shaoy");
        Assert.assertNotNull(dc);
        DbCluster dbCluster = dc.getDbClusters().get("integration-test.fat-fx-drc1");
        Assert.assertNotNull(dbCluster);
        List<Replicator> actualReplicators = dbCluster.getReplicators();
        List<Applier> roughActualAppliers = dbCluster.getAppliers();
        List<Applier> actualAppliers = roughActualAppliers.stream().filter(p -> p.getTargetMhaName().equalsIgnoreCase("fat-fx-drc2")).collect(Collectors.toList());
        Assert.assertEquals(2, actualReplicators.size());
        Assert.assertEquals(4, roughActualAppliers.size());
        Assert.assertEquals(2, actualAppliers.size());
        for(Replicator replicator : actualReplicators) {
            Assert.assertTrue(expectedReplicatorIps.contains(replicator.getIp()));
        }
        for(Applier applier : actualAppliers) {
            Assert.assertTrue(expectedApplierIps.contains(applier.getIp()));
        }
        
    }

    // this test is based on the MetaGeneratorTest.testRouteInfo, may need to make it standalone later
    @Test
    public void testSubmitProxyRouteConfig() throws SQLException {
        Long routeOrgId = dalUtils.getId(TableEnum.BU_TABLE, "BBZ");
        Long srcDcId = dalUtils.getId(TableEnum.DC_TABLE, "shaoy");
        Long dstDcId = dalUtils.getId(TableEnum.DC_TABLE, "sharb");

        Long proxyTlsOy1Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXYTLS_DC1_1);
        Long proxyOy1Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXY_DC1_1);
        Long proxyTlsOy2Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXYTLS_DC1_2);
        Long proxyOy2Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXY_DC1_2);
        Long proxyTlsRb1Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXYTLS_DC2_1);
        Long proxyRb1Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXY_DC2_1);
        Long proxyTlsRb2Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXYTLS_DC2_2);
        Long proxyRb2Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXY_DC2_2);
        Long proxyRelayDcId = dalUtils.getId(TableEnum.PROXY_TABLE, PROXY_DC_RELAY);

        RouteDto routeDto = new RouteDto();
        routeDto.setId(0L);
        routeDto.setRouteOrgName("BBZ");
        routeDto.setSrcDcName("shaoy");
        routeDto.setDstDcName("sharb");
        routeDto.setSrcProxyUris(Arrays.asList(PROXY_DC1_1, PROXY_DC1_2));
        routeDto.setRelayProxyUris(Arrays.asList(PROXY_DC_RELAY));
        routeDto.setDstProxyUris(Arrays.asList(PROXYTLS_DC2_1));
        routeDto.setTag("meta");
        drcBuildService.submitProxyRouteConfig(routeDto);
        RouteTbl routeTbl = dalUtils.getRouteTblDao().queryAll().stream().filter(p -> p.getRouteOrgId().equals(routeOrgId) && p.getSrcDcId().equals(srcDcId) && p.getDstDcId().equals(dstDcId)).findFirst().get();
        Assert.assertEquals(String.format("%s,%s", proxyOy1Id, proxyOy2Id), routeTbl.getSrcProxyIds());
        Assert.assertEquals(String.format("%s", proxyRelayDcId), routeTbl.getOptionalProxyIds());
        Assert.assertEquals(String.format("%s", proxyTlsRb1Id), routeTbl.getDstProxyIds());

        // change back for future use
        routeDto.setDstProxyUris(Arrays.asList(PROXYTLS_DC2_1, PROXYTLS_DC2_2));
        drcBuildService.submitProxyRouteConfig(routeDto);
        routeTbl = dalUtils.getRouteTblDao().queryAll().stream().filter(p -> p.getRouteOrgId().equals(routeOrgId) && p.getSrcDcId().equals(srcDcId) && p.getDstDcId().equals(dstDcId)).findFirst().get();
        Assert.assertEquals(String.format("%s,%s", proxyOy1Id, proxyOy2Id), routeTbl.getSrcProxyIds());
        Assert.assertEquals(String.format("%s,%s", proxyTlsRb1Id, proxyTlsRb2Id), routeTbl.getDstProxyIds());
    }
    
    @Test
    public void testPreCheckBeforeBuild() throws SQLException {
        MetaProposalDto metaProposalDto = new MetaProposalDto();
        metaProposalDto.setSrcMha(MHA1OY);
        metaProposalDto.setDestMha(MHA1RB);
        
        //case1
        List<String> srcReplicators = Lists.newArrayList("10.2.83.105","10.2.87.153");
        List<String> destReplicators = Lists.newArrayList("10.2.83.106","10.2.86.199");
        // one mock for 3 case
        Mockito.doReturn(srcReplicators).when(metaInfoService).getResourcesInUse(Mockito.eq(MHA1OY),Mockito.eq(MHA1RB),Mockito.eq( ModuleEnum.REPLICATOR.getDescription()));
        metaProposalDto.setSrcReplicatorIps(srcReplicators);
        metaProposalDto.setDestReplicatorIps(destReplicators);
        DrcBuildPreCheckVo drcBuildPreCheckVo = drcBuildService.preCheckBeforeBuild(metaProposalDto);
        Assert.assertEquals(DrcBuildPreCheckVo.NO_CONFLICT,drcBuildPreCheckVo.getStatus());
        
        //case2
        List<String> srcReplicators2 = Lists.newArrayList("10.2.87.153");
        List<String> destReplicators2 = Lists.newArrayList("10.2.83.106","10.2.86.199");
        metaProposalDto.setSrcReplicatorIps(srcReplicators2);
        metaProposalDto.setDestReplicatorIps(destReplicators2);
        DrcBuildPreCheckVo drcBuildPreCheckVo2 = drcBuildService.preCheckBeforeBuild(metaProposalDto);
        Assert.assertEquals(DrcBuildPreCheckVo.CONFLICT,drcBuildPreCheckVo2.getStatus());
        Assert.assertEquals(MHA1OY,drcBuildPreCheckVo2.getConflictMha());

        //case3
        metaProposalDto.setSrcMha(MHA1RB);
        metaProposalDto.setDestMha(MHA1OY);
        List<String> srcReplicators3 = Lists.newArrayList("10.2.83.106","10.2.86.199");
        List<String> destReplicators3 = Lists.newArrayList("10.2.87.153");
        metaProposalDto.setSrcReplicatorIps(srcReplicators3);
        metaProposalDto.setDestReplicatorIps(destReplicators3);
        DrcBuildPreCheckVo drcBuildPreCheckVo3 = drcBuildService.preCheckBeforeBuild(metaProposalDto);
        Assert.assertEquals(DrcBuildPreCheckVo.CONFLICT,drcBuildPreCheckVo3.getStatus());
        Assert.assertEquals(MHA1OY,drcBuildPreCheckVo3.getConflictMha());
        
    }
}

package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.service.DataMediaService;
import com.ctrip.framework.drc.console.service.MessengerService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.Arrays;

public class DrcBuildServiceImplTest extends AbstractTest {

    @InjectMocks
    private DrcBuildServiceImpl drcBuildService;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private DataCenterService dataCenterService;

    @Mock
    private RowsFilterService rowsFilterService;
    @Mock
    private DataMediaService dataMediaService;

    @Mock
    private MessengerService messengerService;
    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;
    @Mock
    private MetaProviderV2 metaProviderV2;
    @Mock
    private DbClusterSourceProvider metaProviderV1;
    @Mock
    private MhaTblV2Dao mhaTblV2Dao;

    private DalUtils dalUtils = DalUtils.getInstance();

    public static String DC1 = "shaoy";
    public static String DC2 = "sharb";
    public static String DC_RELAY = "shajq";
    public static String PROXYTLS = "PROXYTLS";
    public static String PROXY = "PROXY";
    public static String IP_DC1_1 = "100.100.100.1";
    public static String IP_DC1_2 = "100.100.100.2";
    public static String IP_DC2_1 = "100.100.100.3";
    public static String IP_DC2_2 = "100.100.100.4";
    public static String IP_DC_RELAY = "100.100.100.5";
    public static String PORT_TLS = "443";
    public static String PORT_IN = "80";

    public static String PROXYTLS_DC1_1 = String.format("%s://%s:%s", PROXYTLS, IP_DC1_1, PORT_TLS);
    public static String PROXY_DC1_1 = String.format("%s://%s:%s", PROXY, IP_DC1_1, PORT_IN);
    public static String PROXYTLS_DC1_2 = String.format("%s://%s:%s", PROXYTLS, IP_DC1_2, PORT_TLS);
    public static String PROXY_DC1_2 = String.format("%s://%s:%s", PROXY, IP_DC1_2, PORT_IN);
    public static String PROXYTLS_DC2_1 = String.format("%s://%s:%s", PROXYTLS, IP_DC2_1, PORT_TLS);
    public static String PROXY_DC2_1 = String.format("%s://%s:%s", PROXY, IP_DC2_1, PORT_IN);
    public static String PROXYTLS_DC2_2 = String.format("%s://%s:%s", PROXYTLS, IP_DC2_2, PORT_TLS);
    public static String PROXY_DC2_2 = String.format("%s://%s:%s", PROXY, IP_DC2_2, PORT_IN);
    public static String PROXY_DC_RELAY = String.format("%s://%s:%s", PROXYTLS, IP_DC_RELAY, PORT_TLS);


    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn(DC1).when(dataCenterService).getDc();
        Mockito.when(consoleConfig.getPublicCloudRegion()).thenReturn(Sets.newHashSet("cloudRegion"));
        Mockito.when(consoleConfig.getRegionForDc(Mockito.anyString())).thenReturn("sha");
        Mockito.when(rowsFilterService.generateRowsFiltersConfig(Mockito.anyLong(), Mockito.eq(0))).thenReturn(null);
        Mockito.when(messengerService.generateMessengers(Mockito.anyLong())).thenReturn(Lists.newArrayList());
        Mockito.when(dataMediaService.generateConfig(Mockito.anyLong())).thenReturn(new DataMediaConfig());
        Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.anyString())).thenReturn(null);
        Mockito.when(consoleConfig.getDrcDoubleWriteSwitch()).thenReturn("off");
        Mockito.doNothing().when(metaProviderV1).scheduledTask();
        Mockito.doNothing().when(metaProviderV2).scheduledTask();
        Mockito.when(mhaTblV2Dao.queryByPk(Mockito.anyLong())).thenReturn(new MhaTblV2());
        Mockito.when(mhaTblV2Dao.update(Mockito.any(MhaTblV2.class))).thenReturn(0);

    }

    // this test is based on the MetaGeneratorTest.testRouteInfo, may need to make it standalone later
    @Test
    //ql_deng TODO 2023/12/1: need change do not base on other test
    public void testSubmitProxyRouteConfig() throws SQLException {
        Long routeOrgId = dalUtils.getId(TableEnum.BU_TABLE, "BBZ");
        Long srcDcId = dalUtils.getId(TableEnum.DC_TABLE, "shaoy");
        Long dstDcId = dalUtils.getId(TableEnum.DC_TABLE, "sharb");

        Long proxyOy1Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXY_DC1_1);
        Long proxyOy2Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXY_DC1_2);
        Long proxyTlsRb1Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXYTLS_DC2_1);
        Long proxyTlsRb2Id = dalUtils.getId(TableEnum.PROXY_TABLE, PROXYTLS_DC2_2);
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
}

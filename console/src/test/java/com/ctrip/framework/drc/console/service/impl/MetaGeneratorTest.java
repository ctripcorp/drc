package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.delay.config.DataCenterService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.entity.Dc;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.entity.Route;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class MetaGeneratorTest {

    @InjectMocks
    private MetaGenerator metaGenerator;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private DataCenterService dataCenterService;
    
    @Mock
    private RowsFilterService rowsFilterService;

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
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn(DC1).when(dataCenterService).getDc();
        Mockito.doReturn(new HashSet<>()).when(consoleConfig).getPublicCloudDc();
        Mockito.doReturn(null).when(rowsFilterService).generateRowsFiltersConfig(Mockito.anyLong());
    }

    // this test needs to be run after TransferServiceImplTest
    @Test
    public void testRouteInfo() throws Exception {
        // init
        List<DcTbl> dcTbls = dalUtils.getDcTblDao().queryAll().stream().filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode())).collect(Collectors.toList());
        Long oyId = dcTbls.stream().filter(p -> p.getDcName().equalsIgnoreCase("shaoy")).findFirst().get().getId();
        Long rbId = dcTbls.stream().filter(p -> p.getDcName().equalsIgnoreCase("sharb")).findFirst().get().getId();
        Long buId = dalUtils.getId(TableEnum.BU_TABLE, "BBZ");
        dalUtils.updateOrCreateProxy(new ProxyDto(DC1, PROXYTLS, IP_DC1_1, PORT_TLS).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC1, PROXY, IP_DC1_1, PORT_IN).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC1, PROXYTLS, IP_DC1_2, PORT_TLS).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC1, PROXY, IP_DC1_2, PORT_IN).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC2, PROXYTLS, IP_DC2_1, PORT_TLS).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC2, PROXY, IP_DC2_1, PORT_IN).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC2, PROXYTLS, IP_DC2_2, PORT_TLS).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC2, PROXY, IP_DC2_2, PORT_IN).toProxyTbl());
        dalUtils.updateOrCreateProxy(new ProxyDto(DC_RELAY, PROXYTLS, IP_DC_RELAY, PORT_TLS).toProxyTbl());
        List<ProxyTbl> proxyTbls = dalUtils.getProxyTblDao().queryAll();
        Long proxyTlsOy1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC1_1)).findFirst().get().getId();
        Long proxyOy1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC1_1)).findFirst().get().getId();
        Long proxyTlsOy2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC1_2)).findFirst().get().getId();
        Long proxyOy2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC1_2)).findFirst().get().getId();
        Long proxyTlsRb1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC2_1)).findFirst().get().getId();
        Long proxyRb1Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC2_1)).findFirst().get().getId();
        Long proxyTlsRb2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXYTLS_DC2_2)).findFirst().get().getId();
        Long proxyRb2Id = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC2_2)).findFirst().get().getId();
        Long proxyRelayDcId = proxyTbls.stream().filter(p -> p.getUri().equalsIgnoreCase(PROXY_DC_RELAY)).findFirst().get().getId();

        dalUtils.insertRoute(buId, oyId, rbId, String.format("%s,%s", proxyOy1Id, proxyOy2Id), proxyRelayDcId.toString(), String.format("%s,%s", proxyTlsRb1Id, proxyTlsRb2Id), "meta");
        dalUtils.insertRoute(buId, oyId, rbId, String.format("%s", proxyOy1Id), "", String.format("%s", proxyTlsRb1Id), "console");
        dalUtils.insertRoute(buId, rbId, oyId, String.format("%s,%s", proxyRb1Id, proxyRb2Id), "", String.format("%s,%s", proxyTlsOy1Id, proxyTlsOy2Id), "meta");
        dalUtils.insertRoute(buId, rbId, oyId, String.format("%s", proxyRb1Id), "", String.format("%s", proxyTlsOy1Id), "console");
        RouteTbl routeTbl = dalUtils.getRouteTblDao().queryAll().stream()
                .filter(p -> p.getRouteOrgId().equals(buId) &&
                        p.getSrcDcId().equals(rbId) &&
                        p.getDstDcId().equals(oyId) &&
                        p.getTag().equalsIgnoreCase("meta"))
                .findFirst().get();
        routeTbl.setDeleted(BooleanEnum.TRUE.getCode());
        dalUtils.getRouteTblDao().update(routeTbl);

        Drc drc = metaGenerator.getDrc();
        Dc oyDc = drc.findDc("shaoy");
        List<Route> routes = oyDc.getRoutes();
        Assert.assertEquals(2, routes.size());
        for(Route route : routes) {
            Assert.assertEquals(buId.intValue(), route.getOrgId().intValue());
            Assert.assertEquals("shaoy", route.getSrcDc());
            Assert.assertEquals("sharb", route.getDstDc());
            Assert.assertTrue("meta".equalsIgnoreCase(route.getTag()) || "console".equalsIgnoreCase(route.getTag()));
            if("meta".equalsIgnoreCase(route.getTag())) {
                Assert.assertEquals(String.format("PROXY://%s:80,PROXY://%s:80 PROXYTLS://%s:443 PROXYTLS://%s:443,PROXYTLS://%s:443", IP_DC1_1, IP_DC1_2, IP_DC_RELAY, IP_DC2_1, IP_DC2_2), route.getRouteInfo());
            } else {
                Assert.assertEquals(String.format("PROXY://%s:80 PROXYTLS://%s:443", IP_DC1_1, IP_DC2_1), route.getRouteInfo());
            }
        }

        Dc rbDc = drc.findDc("sharb");
        routes = rbDc.getRoutes();
        Assert.assertEquals(1, routes.size());
        Route route = routes.get(0);
        Assert.assertEquals(buId.intValue(), route.getOrgId().intValue());
        Assert.assertEquals("sharb", route.getSrcDc());
        Assert.assertEquals("shaoy", route.getDstDc());
        Assert.assertTrue("console".equalsIgnoreCase(route.getTag()));
        Assert.assertEquals(String.format("PROXY://%s:80 PROXYTLS://%s:443", IP_DC2_1, IP_DC1_1), route.getRouteInfo());
    }
}

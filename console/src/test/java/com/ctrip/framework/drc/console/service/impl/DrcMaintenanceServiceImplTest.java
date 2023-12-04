package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.MhaGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.doNothing;

public class DrcMaintenanceServiceImplTest extends AbstractTest {

    private DalUtils dalUtils = DalUtils.getInstance();

    @InjectMocks
    private DrcMaintenanceServiceImpl drcMaintenanceService;

    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;

    @Mock
    private DefaultCurrentMetaManager currentMetaManager;
    
    @Mock
    private MachineTblDao machineTblDao;

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
        doNothing().when(currentMetaManager).addSlaveMySQL(Mockito.anyString(), Mockito.any());
        MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(1L);
        Mockito.doReturn("off").when(monitorTableSourceProvider).getSlaveMachineOfflineSyncSwitch();
    }

    //ql_deng TODO 2023/12/4:
    @Test
    public void testDeleteRoute() throws SQLException {
        Long oyId = dalUtils.getDcTblDao().queryAll().stream().filter(p -> p.getDcName().equalsIgnoreCase("shaoy")).findFirst().get().getId();
        Long rbId = dalUtils.getDcTblDao().queryAll().stream().filter(p -> p.getDcName().equalsIgnoreCase("sharb")).findFirst().get().getId();
        Long buId = dalUtils.getId(TableEnum.BU_TABLE, "BBZ");

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
        // init check before delete
        List<RouteTbl> routeTbls = dalUtils.getRouteTblDao().queryAll().stream()
                .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) &&
                        p.getRouteOrgId().equals(buId) &&
                        p.getSrcDcId().equals(rbId) &&
                        p.getDstDcId().equals(oyId) &&
                        p.getTag().equalsIgnoreCase("console"))
                .collect(Collectors.toList());
        Assert.assertEquals(1, routeTbls.size());
        RouteTbl routeTbl = routeTbls.get(0);
        Assert.assertNotNull(routeTbl);
        Assert.assertEquals(buId, routeTbl.getRouteOrgId());
        Assert.assertEquals(rbId, routeTbl.getSrcDcId());
        Assert.assertEquals(oyId, routeTbl.getDstDcId());
        Assert.assertTrue("console".equalsIgnoreCase(routeTbl.getTag()));
        Assert.assertEquals(String.format("%s", proxyRb1Id), routeTbl.getSrcProxyIds());
        Assert.assertEquals(String.format("%s", proxyTlsOy1Id), routeTbl.getDstProxyIds());

        ApiResult result = drcMaintenanceService.deleteRoute("BBZ", "sharb", "shaoy", "console");
        Assert.assertEquals(0, result.getStatus().intValue());
        // check after success
        routeTbl = dalUtils.getRouteTblDao().queryAll().stream()
                .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) &&
                        p.getRouteOrgId().equals(buId) &&
                        p.getSrcDcId().equals(rbId) &&
                        p.getDstDcId().equals(oyId) &&
                        p.getTag().equalsIgnoreCase("console"))
                .findFirst().orElse(null);
        Assert.assertNull(routeTbl);
    }

    @Test
    public void testInputDc() throws SQLException {
        ApiResult result = drcMaintenanceService.inputDc("dc1");
        Assert.assertEquals(ResultCode.HANDLE_SUCCESS.getCode(), result.getStatus().intValue());
        Long id = dalUtils.getId(TableEnum.DC_TABLE, "dc1");
        Assert.assertNotNull(id);
        id = dalUtils.getId(TableEnum.DC_TABLE, "dc2");
        Assert.assertNull(id);
    }

}

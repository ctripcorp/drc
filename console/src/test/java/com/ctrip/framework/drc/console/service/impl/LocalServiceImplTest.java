package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.List;
import java.util.Map;


public class LocalServiceImplTest extends AbstractTest {
    
    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;
    
    @InjectMocks
    private LocalServiceImpl localService;
    
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        MySqlEndpoint endpoint1 = new MySqlEndpoint("ip1", 3306, "usr1", "psw1", true);
        MySqlEndpoint endpoint2 = new MySqlEndpoint("ip1", 3306, "usr2", "psw2", true);
        MySqlEndpoint endpoint3 = new MySqlEndpoint("ip1", 3306, "usr3", "psw3", true);
        List<Endpoint> endpoints =Lists.newArrayList(endpoint1,endpoint2,endpoint3);
        Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.eq("mha1"))).thenReturn(endpoint1);
        Mockito.when(dbClusterSourceProvider.getMasterEndpointsInAllAccounts(Mockito.eq("mha1"))).thenReturn(endpoints);
    }

    @Test
    public void testPreCheckMySqlConfig() {
        try (MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.checkBinlogMode(Mockito.any())).thenReturn("ON");
            theMock.when(() -> MySqlUtils.checkBinlogFormat(Mockito.any())).thenReturn("ROW");
            theMock.when(() -> MySqlUtils.checkBinlogVersion(Mockito.any())).thenReturn("OFF");
            theMock.when(() -> MySqlUtils.checkBinlogTransactionDependency(Mockito.any())).thenReturn("WRITESET");
            theMock.when(() -> MySqlUtils.checkGtidMode(Mockito.any())).thenReturn("ON");
            theMock.when(() -> MySqlUtils.checkAutoIncrementStep(Mockito.any())).thenReturn(2);
            theMock.when(() -> MySqlUtils.checkAutoIncrementOffset(Mockito.any())).thenReturn(1);
            theMock.when(() -> MySqlUtils.checkAccounts(Mockito.any())).thenReturn("three accounts ready");
            
            Map<String, Object> res = localService.preCheckMySqlConfig("mha1");
            Assert.assertEquals(9,res.size());
            
            Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.eq("mha1"))).thenReturn(null);
            res = localService.preCheckMySqlConfig("mha1");
            Assert.assertEquals(0,res.size());
            
            MySqlEndpoint endpoint1 = new MySqlEndpoint("ip1", 3306, "usr1", "psw1", true);
            Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.eq("mha1"))).thenReturn(endpoint1);
            Mockito.when(dbClusterSourceProvider.getMasterEndpointsInAllAccounts(Mockito.eq("mha1"))).thenReturn(null);
            res = localService.preCheckMySqlConfig("mha1");
            Assert.assertEquals("no db endpoint find",res.get("drcAccounts"));
        }
    }

    @Test
    public void testPreCheckMySqlTables() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.checkTablesWithFilter(Mockito.any(),Mockito.anyString())).thenReturn(null);
            List<TableCheckVo> checkVos = localService.preCheckMySqlTables("mha1", ".*");
            Assert.assertNull(checkVos);
            
            Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.eq("mha1"))).thenReturn(null);
             checkVos = localService.preCheckMySqlTables("mha1", ".*");
            Assert.assertEquals(0,checkVos.size());
        }
    }
}
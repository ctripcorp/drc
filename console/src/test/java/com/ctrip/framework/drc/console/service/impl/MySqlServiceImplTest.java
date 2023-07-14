package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.AbstractTest;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;


public class MySqlServiceImplTest extends AbstractTest {

    @InjectMocks
    private MySqlServiceImpl mySqlService;
    
    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    private final MySqlEndpoint endpoint = new MySqlEndpoint(
            "127.0.0.1", 3306, "root", "", true
    );

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.eq("mha1"))).
                thenReturn(endpoint);
    }

    @Test
    public void testGetCreateTableStatements() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> {
                MySqlUtils.getDefaultCreateTblStmts(Mockito.any(Endpoint.class),Mockito.any(AviatorRegexFilter.class));
            }).thenReturn(null);
            mySqlService.getCreateTableStatements("mha1","db1\\..*",endpoint);
        }
    }

    @Test
    public void testGetAutoIncrement() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> {
                MySqlUtils.getSqlResultInteger(Mockito.any(Endpoint.class),Mockito.anyString(),Mockito.anyInt());
            }).thenReturn(1);
            mySqlService.getAutoIncrement("mha1","querySql",1,endpoint);
        }
    }

    @Test
    public void testGetRealExecutedGtid() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> {
                MySqlUtils.getUnionExecutedGtid(Mockito.any(Endpoint.class));
            }).thenReturn("gtid");
            String gtid = mySqlService.getDrcExecutedGtid("mha1");
            Assert.assertEquals(gtid,"gtid");
            Mockito.when(dbClusterSourceProvider.getMasterEndpoint("mha1")).
                    thenReturn(null);
            gtid = mySqlService.getDrcExecutedGtid("mha1");
            Assert.assertNull(gtid);
            
        }
    }
        
}
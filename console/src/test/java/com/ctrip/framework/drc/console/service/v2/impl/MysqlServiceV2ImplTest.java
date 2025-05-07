package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.param.mysql.DrcDbMonitorTableCreateReq;
import com.ctrip.framework.drc.console.service.v2.CacheMetaService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.AccountEndpoint;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class MysqlServiceV2ImplTest {
    
    @InjectMocks
    MysqlServiceV2Impl mysqlServiceV2;
    @Mock
    CacheMetaService cacheMetaService;
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void getExistDrcMonitorTables() {
        List<String> tables = new ArrayList<>();
        tables.add("dly_db1");
        tables.add("tx_db1");
        tables.add("dly_db2");
        tables.add("tx_db3");
        Set<String> existDrcMonitorTables = MySqlUtils.getDbHasDrcMonitorTables(tables);
        Assert.assertTrue(existDrcMonitorTables.contains("db1"));
        Assert.assertFalse(existDrcMonitorTables.contains("db2"));
        Assert.assertFalse(existDrcMonitorTables.contains("db3"));
        Assert.assertFalse(existDrcMonitorTables.contains("db4"));
        Assert.assertEquals(1, existDrcMonitorTables.size());
        Assert.assertEquals(0, MySqlUtils.getDbHasDrcMonitorTables(Lists.newArrayList()).size());
    }

    // todo hdpan  
    @Test
    public void testCheckAccountsPrivileges() {
        when(cacheMetaService.getMasterEndpoint(Mockito.eq("mhaName"))).thenReturn(new MySqlEndpoint("host", 3306, "user", "password",true));
        try (MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getAccountPrivilege(Mockito.any(AccountEndpoint.class),Mockito.anyBoolean())).thenAnswer(
                    invocation -> {
                        AccountEndpoint accountEndpoint = invocation.getArgument(0);
                        String privilegeFormatter = "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `%s`@`%%`";
                        return String.format(privilegeFormatter, accountEndpoint.getUser());
                    }
            );
            String s = mysqlServiceV2.queryAccountPrivileges("mhaName", "user", "password");
            Assert.assertNotNull(s);
        }
    }

    @Test //should create messenger_gitd_tbl and db tbl
    public void testCreateDrcMonitorDbTable() throws Exception{
        when(cacheMetaService.getMasterEndpoint(Mockito.eq("mhaName"))).thenReturn(new MySqlEndpoint("host", 3306, "user", "password",true));
        try (MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getTablesFromDb(Mockito.any(AccountEndpoint.class), anyString())).thenAnswer(
                     invocation -> Lists.newArrayList()
            );
            theMock.when(() -> MySqlUtils.getDbHasDrcMonitorTables(Mockito.any(List.class))).thenAnswer(
                    invocation -> Sets.newHashSet()
            );


            DrcDbMonitorTableCreateReq req = new DrcDbMonitorTableCreateReq("mhaName", Lists.newArrayList("db1"));
            Boolean ans = mysqlServiceV2.createDrcMonitorDbTable(req);
            Assert.assertFalse(ans);
        }
    }

    @Test //should create messenger_gitd_tbl
    public void testCreateDrcMonitorDbTable2() throws Exception{
        when(cacheMetaService.getMasterEndpoint(Mockito.eq("mhaName"))).thenReturn(new MySqlEndpoint("host", 3306, "user", "password",true));
        try (MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getTablesFromDb(Mockito.any(AccountEndpoint.class), anyString())).thenAnswer(
                    invocation -> Lists.newArrayList("db1")
            );
            theMock.when(() -> MySqlUtils.getDbHasDrcMonitorTables(Mockito.any(List.class))).thenAnswer(
                    invocation -> {
                        Set<String> s = Sets.newHashSet();
                        s.add("db1");
                        return s;
                    }
            );


            DrcDbMonitorTableCreateReq req = new DrcDbMonitorTableCreateReq("mhaName", Lists.newArrayList("db1"));
            Boolean ans = mysqlServiceV2.createDrcMonitorDbTable(req);
            Assert.assertFalse(ans);
        }
    }

    @Test //no need create
    public void testCreateDrcMonitorDbTable3() throws Exception{
        when(cacheMetaService.getMasterEndpoint(Mockito.eq("mhaName"))).thenReturn(new MySqlEndpoint("host", 3306, "user", "password",true));
        try (MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getTablesFromDb(Mockito.any(Endpoint.class), anyString())).thenAnswer(
                    invocation -> Lists.newArrayList("db1","messenger_gtid_executed")
            );
            theMock.when(() -> MySqlUtils.getDbHasDrcMonitorTables(Mockito.any(List.class))).thenAnswer(
                    invocation -> {
                        Set<String> s = Sets.newHashSet();
                        s.add("db1");
                        return s;
                    }
            );


            DrcDbMonitorTableCreateReq req = new DrcDbMonitorTableCreateReq("mhaName", Lists.newArrayList("db1"));
            Boolean ans = mysqlServiceV2.createDrcMonitorDbTable(req);
            Assert.assertTrue(ans);
        }
    }

    @Test ////should create db tbl
    public void testCreateDrcMonitorDbTable4() throws Exception{
        when(cacheMetaService.getMasterEndpoint(Mockito.eq("mhaName"))).thenReturn(new MySqlEndpoint("host", 3306, "user", "password",true));
        try (MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getTablesFromDb(Mockito.any(Endpoint.class), anyString())).thenAnswer(
                    invocation -> Lists.newArrayList("messenger_gtid_executed")
            );
            theMock.when(() -> MySqlUtils.getDbHasDrcMonitorTables(Mockito.any(List.class))).thenAnswer(
                    invocation -> Sets.newHashSet()
            );


            DrcDbMonitorTableCreateReq req = new DrcDbMonitorTableCreateReq("mhaName", Lists.newArrayList("db1"));
            Boolean ans = mysqlServiceV2.createDrcMonitorDbTable(req);
            Assert.assertFalse(ans);
        }
    }

}
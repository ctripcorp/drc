package com.ctrip.framework.drc.console.service.v2.impl;

import com.alibaba.fastjson.JSON;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.MessengerMetaDto;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.response.QmqApiResponse;
import com.ctrip.framework.drc.console.vo.response.QmqBuEntity;
import com.ctrip.framework.drc.console.vo.response.QmqBuList;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

public class MessengerServiceV2ImplTest extends CommonDataInit {
    public static final String VPC_MHA_NAME = "vpcMha1";

    @Mock
    DbClusterApiService dbClusterService;

    @Before
    public void setUp() throws IOException, SQLException {
        MockitoAnnotations.openMocks(this);
        when(defaultConsoleConfig.getVpcMhaNames()).thenReturn(Lists.newArrayList(VPC_MHA_NAME));
        super.setUp();
    }


    @Test
    public void testGetAllMessengerMhaTbls() throws Exception {
        List<MhaTblV2> result = messengerServiceV2Impl.getAllMessengerMhaTbls();
        Assert.assertEquals(result.size(), 3);
        List<Long> ids = result.stream().map(MhaTblV2::getId).collect(Collectors.toList());
        Assert.assertTrue(ids.contains(1L));
        Assert.assertTrue(ids.contains(2L));
    }

    @Test
    public void testQueryMhaMessengerConfigs() throws Exception {
        List<MqConfigVo> mq1 = messengerServiceV2Impl.queryMhaMessengerConfigs("mha1");
        Assert.assertEquals(4, mq1.size());
        System.out.println(mq1);
        System.out.println(JSON.toJSONString(mq1));
        Assert.assertTrue(mq1.stream().allMatch(e -> e.getTopic().equals("bbz.mha1.binlog")));
        Assert.assertTrue(mq1.stream().allMatch(e -> Lists.newArrayList("db1\\.(table1|table2)", "db2\\.(table1|table2)", "db1\\.(table3|table4)", "db3\\.(table1|table2)").contains(e.getTable())));

        List<MqConfigVo> mq2 = messengerServiceV2Impl.queryMhaMessengerConfigs("mha2");
        Assert.assertEquals(1, mq2.size());
        List<MqConfigVo> mq3 = messengerServiceV2Impl.queryMhaMessengerConfigs("mha3");
        Assert.assertEquals(0, mq3.size());
    }

    @Test
    public void testGetBusFromQmq() throws Exception {
        QmqBuEntity bu1 = new QmqBuEntity();
        bu1.setEnName("bbz");
        QmqBuEntity bu2 = new QmqBuEntity();
        bu2.setEnName("test");
        List<QmqBuEntity> qmqBuEntities = Lists.newArrayList(bu1, bu2);
        List<String> buStrings = qmqBuEntities.stream().map(QmqBuEntity::getEnName).collect(Collectors.toList());
        QmqBuList response = new QmqBuList();
        response.setData(qmqBuEntities);

        when(domainConfig.getQmqBuListUrl()).thenReturn("something");

        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            mocked.when(() -> HttpUtils.post(anyString(), any(), any())).thenReturn(response);
            List<String> result = messengerServiceV2Impl.getBusFromQmq();
            Assert.assertEquals(buStrings, result);
        }

    }

    @Test(expected = ConsoleException.class)
    public void testDeleteDbReplicationForMqMhaNotExist() throws Exception {
        messengerServiceV2Impl.deleteDbReplicationForMq("mhaNameNotExist", List.of(Long.valueOf(1)));
    }

    @Test(expected = ConsoleException.class)
    public void testDeleteDbReplicationForMqDbReplicationsNotExist() throws Exception {
        messengerServiceV2Impl.deleteDbReplicationForMq("mha1", List.of(99999999L));
    }


    @Test(expected = ConsoleException.class)
    public void testDeleteDbReplicationForMqDbReplicationsNotMatch() throws Exception {
        messengerServiceV2Impl.deleteDbReplicationForMq("mha1", List.of(1L, 4L));
    }

    @Test(expected = ConsoleException.class)
    public void testDeleteDbReplicationForMqDbReplicationsMqDBType() throws Exception {
        messengerServiceV2Impl.deleteDbReplicationForMq("mha1", List.of(5L));
    }

    @Test
    public void testDeleteDbReplicationForMqNoVpcMha() throws Exception {

        try {
            messengerServiceV2Impl.deleteDbReplicationForMq(VPC_MHA_NAME, List.of(Long.valueOf(1)));
            throw new Exception("should throw exception before it");
        } catch (ConsoleException e) {
            Assert.assertTrue(e.getMessage().contains(ReadableErrorDefEnum.DELETE_TBL_CHECK_FAIL_EXCEPTION.getMessage()));
        }
    }


    @Test
    public void testDeleteDbReplicationForMqDbReplications() throws SQLException {
        List<Long> dbReplicationIds = List.of(1L, 2L);
        messengerServiceV2Impl.deleteDbReplicationForMq("mha1", dbReplicationIds);
        verify(dbReplicationTblDao).queryByIds(anyList());
        verify(mhaTblV2Dao).queryByMhaName(any(), anyInt());
        verify(mhaDbMappingTblDao).queryByIds(anyList());
        verify(dbReplicationFilterMappingTblDao).queryByDbReplicationIds(any());
    }


    @Test
    public void testCheckMqConfigForInsert() {
        MqConfigDto dto = new MqConfigDto();
        // all conflict
        List<MySqlUtils.TableSchemaName> ret1 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table1"),
                new MySqlUtils.TableSchemaName("db1", "table2")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table1|table2)")).thenReturn(ret1);
        dto.setDbReplicationId(null); // insert
        dto.setMhaName("mha1");
        dto.setTable("db1\\.(table1|table2)");
        MqConfigCheckVo result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertFalse(result.getAllowSubmit());
        Assert.assertEquals(2, result.getConflictTables().size());


        // partial conflict
        List<MySqlUtils.TableSchemaName> ret2 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table1"),
                new MySqlUtils.TableSchemaName("db1", "table999")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table1|table999)")).thenReturn(ret2);

        dto.setTable("db1\\.(table1|table999)");
        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertFalse(result.getAllowSubmit());
        Assert.assertEquals(1, result.getConflictTables().size());


        // test no table
        dto.setTable("db1\\.(unpresentTableInDB)");
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.unpresentTableInDB)")).thenReturn(Lists.emptyList());

        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertTrue(result.getAllowSubmit());

        // test table not conflict 1
        List<MySqlUtils.TableSchemaName> ret3 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table999")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table999)")).thenReturn(ret3);
        dto.setTable("db1\\.(table999)");
        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertTrue(result.getAllowSubmit());

        // test table not conflict 2
        List<MySqlUtils.TableSchemaName> ret4 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db999", "table1")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db999\\.(table1)")).thenReturn(ret4);
        dto.setTable("db999\\.(table1)");
        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertTrue(result.getAllowSubmit());
    }


    @Test
    public void testCheckMqConfigForUpdate() {
        MqConfigDto dto = new MqConfigDto();
        // all conflict
        List<MySqlUtils.TableSchemaName> ret1 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table1"),
                new MySqlUtils.TableSchemaName("db1", "table2")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table1|table2)")).thenReturn(ret1);
        dto.setMhaName("mha1");
        dto.setTable("db1\\.(table1|table2)");
        dto.setDbReplicationId(3L);
        MqConfigCheckVo result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertFalse(result.getAllowSubmit());
        Assert.assertEquals(2, result.getConflictTables().size());


        // partial conflict
        List<MySqlUtils.TableSchemaName> ret2 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table2"),
                new MySqlUtils.TableSchemaName("db1", "table3")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table2|table3)")).thenReturn(ret2);
        dto.setMhaName("mha1");
        dto.setTable("db1\\.(table2|table3)");
        dto.setDbReplicationId(1L);
        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertFalse(result.getAllowSubmit());
        Assert.assertEquals(1, result.getConflictTables().size());


        // no conflict: different db
        List<MySqlUtils.TableSchemaName> ret3 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db999", "table1"),
                new MySqlUtils.TableSchemaName("db999", "table2")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db999\\.(table1|table2)")).thenReturn(ret3);
        dto.setMhaName("mha1");
        dto.setTable("db999\\.(table1|table2)");
        dto.setDbReplicationId(1L);
        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertTrue(result.getAllowSubmit());

        // no conflict: different table
        List<MySqlUtils.TableSchemaName> ret4 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table999")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table999)")).thenReturn(ret4);
        dto.setMhaName("mha1");
        dto.setTable("db1\\.(table999)");
        dto.setDbReplicationId(1L);
        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertTrue(result.getAllowSubmit());


        // no conflict: update itself
        List<MySqlUtils.TableSchemaName> ret5 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table1"));
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table1)")).thenReturn(ret5);
        dto.setMhaName("mha1");
        dto.setTable("db1\\.(table1)");
        dto.setDbReplicationId(1L);
        result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
        Assert.assertTrue(result.getAllowSubmit());
    }


    @Test(expected = ConsoleException.class)
    public void testCheckMqConfigForUpdateMhaDbNotMatch() throws Exception {
        MqConfigDto dto = new MqConfigDto();
        // all conflict
        List<MySqlUtils.TableSchemaName> ret1 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table1"),
                new MySqlUtils.TableSchemaName("db1", "table2")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table1|table2)")).thenReturn(ret1);
        dto.setMhaName("mha1");
        dto.setTable("db1\\.(table1|table2)");
        dto.setDbReplicationId(999L);
        MqConfigCheckVo result = messengerServiceV2Impl.checkMqConfig(dto);
        System.out.println(JSON.toJSONString(result));
    }

    @Test
    public void testGetMessengerGtidExecuted() throws Exception {
        String result = messengerServiceV2Impl.getMessengerGtidExecuted("mha1");
        Assert.assertEquals("gtid1", result);
    }

    @Test(expected = ConsoleException.class)
    public void testGetMessengerGtidExecutedException1() {
        messengerServiceV2Impl.getMessengerGtidExecuted("mhaNotExist");
    }

    @Test(expected = ConsoleException.class)
    public void testGetMessengerGtidExecutedException2() {
        messengerServiceV2Impl.getMessengerGtidExecuted(null);
    }


    // ----------------- test add
    @Test(expected = ConsoleException.class)
    public void testProcessAddMqConfigException1() throws Exception {
        MqConfigDto dto = new MqConfigDto();
        dto.setMhaName("mha999");
        messengerServiceV2Impl.processAddMqConfig(dto);
    }

    @Test(expected = ConsoleException.class)
    public void testProcessAddMqConfigException2() throws Exception {
        MqConfigDto dto = new MqConfigDto();
        dto.setMhaName(VPC_MHA_NAME);
        messengerServiceV2Impl.processAddMqConfig(dto);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testProcessAddMqConfigException3() throws Exception {
        MqConfigDto dto = new MqConfigDto();
        dto.setMhaName("mha1");
        dto.setMqType("notQmq");
        messengerServiceV2Impl.processAddMqConfig(dto);
    }


    @Test
    public void testProcessAddMqConfigAddReplications() throws Exception {

        QmqApiResponse response = new QmqApiResponse();
        response.setStatus(0);
        response.setData(new Object());
        List<MySqlUtils.TableSchemaName> ret2 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table1"),
                new MySqlUtils.TableSchemaName("db1", "table2")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db1\\.(table1|table2)")).thenReturn(ret2);
        when(mysqlServiceV2.queryTablesWithNameFilter("mha1", "db1\\.(table1|table2)")).thenReturn(Lists.newArrayList("db1.table1", "db1.table2"));

        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            mocked.when(() -> HttpUtils.post(any(), any(), any())).thenReturn(response);
            MqConfigDto dto = new MqConfigDto();
            dto.setMhaName("mha1");
            dto.setMqType("qmq");
            dto.setTable("db1\\\\.(table1|table2)");
            messengerServiceV2Impl.processAddMqConfig(dto);
        }
    }


    @Test
    public void testProcessAddMqConfigAddMhaMapping() throws Exception {

        QmqApiResponse response = new QmqApiResponse();
        response.setStatus(0);
        response.setData(new Object());
        List<MySqlUtils.TableSchemaName> ret2 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db3", "table1"),
                new MySqlUtils.TableSchemaName("db3", "table2")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db3\\.(table1|table2)")).thenReturn(ret2);
        when(mysqlServiceV2.queryTablesWithNameFilter("mha2", "db3\\.(table1|table2)")).thenReturn(Lists.newArrayList("db3.table1", "db3.table2"));

        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            mocked.when(() -> HttpUtils.post(any(), any(), any())).thenReturn(response);
            MqConfigDto dto = new MqConfigDto();
            dto.setMhaName("mha2");
            dto.setMqType("qmq");
            dto.setTable("db3\\\\.(table1|table2)");
            messengerServiceV2Impl.processAddMqConfig(dto);
        }
    }


    @Test
    public void testProcessUpdateMqConfig() throws Exception {
        QmqApiResponse response = new QmqApiResponse();
        response.setStatus(0);
        response.setData(new Object());
        List<MySqlUtils.TableSchemaName> ret2 = Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db3", "table1"),
                new MySqlUtils.TableSchemaName("db3", "table2")
        );
        when(mysqlServiceV2.getMatchTable("mha1", "db3\\.(table1|table2)")).thenReturn(ret2);
        when(mysqlServiceV2.getAnyMatchTable("mha1", "db3\\.(table1|table2)")).thenReturn(ret2);
        when(mysqlServiceV2.queryTablesWithNameFilter("mha1", "db3\\.(table1|table2)")).thenReturn(Lists.newArrayList("db3.table1", "db3.table2"));
        when(dbClusterService.getDalClusterName(any(), any())).thenReturn("dalcluster");
        try (MockedStatic<HttpUtils> mocked = mockStatic(HttpUtils.class)) {
            mocked.when(() -> HttpUtils.post(any(), any(), any())).thenReturn(response);
            MqConfigDto dto = new MqConfigDto();
            dto.setMhaName("mha1");
            dto.setTable("db3\\.(table1|table2)");
            dto.setTopic("topic");
            dto.setMqType("qmq");
            dto.setDbReplicationId(7L);
            messengerServiceV2Impl.processUpdateMqConfig(dto);
            verify(qConfigService, times(1)).addOrUpdateDalClusterMqConfig(anyString(), anyString(), anyString(), eq(null), anyList());
            verify(qConfigService, times(1)).updateDalClusterMqConfig(anyString(), anyString(), anyString(), anyList());
        }
    }


    @Test(expected = ConsoleException.class)
    public void testRemoveMessengerGroupException() throws Exception {
        // forbidden operation ( should remove inner mq configs first)
        messengerServiceV2Impl.removeMessengerGroup("mha1");
    }

    @Test
    public void testRemoveMessengerGroup() throws Exception {

        messengerServiceV2Impl.removeMessengerGroup("mha3");
        verify(messengerTblDao, times(1)).batchUpdate(anyList());
        verify(messengerGroupTblDao, times(1)).update(any(MessengerGroupTbl.class));
    }


    @InjectMocks
    DrcBuildServiceV2Impl drcBuildServiceV2;

    @Mock
    MetaInfoServiceV2Impl metaInfoService;
    @Test
    public void testBuildMhaDrc() throws Exception {

        MessengerMetaDto dto = new MessengerMetaDto();
        dto.setMhaName("mha1");
        dto.setReplicatorIps(com.google.common.collect.Lists.newArrayList("1.113.60.1"));
        dto.setMessengerIps(com.google.common.collect.Lists.newArrayList());
        dto.setrGtidExecuted("testRGtidExecuted");
        dto.setaGtidExecuted("testAGtidExecuted");


        when(metaInfoService.getDrcMessengerConfig(anyString())).thenReturn(new Drc());
        drcBuildServiceV2.buildMessengerDrc(dto);

        verify(replicatorTblDao, times(1)).batchInsert(any());
    }

}

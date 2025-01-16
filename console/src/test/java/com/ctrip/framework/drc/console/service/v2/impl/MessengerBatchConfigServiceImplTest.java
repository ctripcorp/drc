package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.DbReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationFilterMappingTblDao;
import com.ctrip.framework.drc.console.dao.v2.DbReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MessengerFilterTblDao;
import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.dto.v3.*;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.DrcAutoBuildParam;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigService;
import com.ctrip.framework.drc.console.service.v2.DrcAutoBuildService;
import com.ctrip.framework.drc.console.service.v2.MessengerServiceV2;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author yongnian
 * @create 2025/1/16 12:42
 */
public class MessengerBatchConfigServiceImplTest {
    @Mock
    DrcAutoBuildService drcAutoBuildService;
    @Mock
    MessengerServiceV2 messengerServiceV2;
    @Mock
    DbReplicationTblDao dbReplicationTblDao;
    @Mock
    DbReplicationFilterMappingTblDao dbReplicationFilterMappingTblDao;
    @Mock
    MessengerFilterTblDao messengerFilterTblDao;
    @Mock
    QConfigService qConfigService;
    @InjectMocks
    MessengerBatchConfigServiceImpl messengerBatchConfigServiceImpl;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        Map<String, List<String>> mockTableMap = new HashMap<>() {{
            put("table1", Lists.newArrayList("table1"));
            put("(table2|table3)", Lists.newArrayList("table2", "table3"));
            put(".*", Lists.newArrayList("table1", "table2", "table3", "table4", "table5"));
        }};

        when(dbReplicationTblDao.batchInsertWithReturnId(anyList())).thenAnswer((e) -> {
            List<DbReplicationTbl> reqs = e.getArgument(0, List.class);
            List<Long> ids = new ArrayList<>();
            for (int i = 0; i < reqs.size(); i++) {
                reqs.get(i).setId((long) i);
            }
            return ids;
        });
        when(drcAutoBuildService.getMatchTable(anyList())).thenAnswer((e) -> {
            List<DrcAutoBuildParam> reqs = e.getArgument(0, List.class);

            List<MySqlUtils.TableSchemaName> res = new ArrayList<>();
            List<String> dbNames = reqs.stream().flatMap(req -> req.getDbName().stream()).sorted().collect(Collectors.toList());
            String logicTable = reqs.get(0).getTableFilter();
            List<String> matchTables = mockTableMap.get(logicTable);
            for (String dbName : dbNames) {
                for (String matchTable : matchTables) {
                    res.add(new MySqlUtils.TableSchemaName(dbName, matchTable));
                }
            }
            return res;
        });
    }

    @Test(expected = ConsoleException.class)
    public void testDeleteException() throws SQLException {
        DbMqEditDto editDto = getDbMqEditDto();
        editDto.getOriginLogicTableConfig().setMessengerFilterId(null);
        DbMqConfigInfoDto currentConfig = getCurrentConfig();

        messengerBatchConfigServiceImpl.processDeleteMqConfig(editDto, currentConfig);
    }

    @Test(expected = ConsoleException.class)
    public void testDelete() throws SQLException {
        DbMqEditDto editDto = getDbMqEditDto();
        DbMqConfigInfoDto currentConfig = getCurrentConfig();

        messengerBatchConfigServiceImpl.processDeleteMqConfig(editDto, currentConfig);
    }


    @Test
    public void testCreate() throws SQLException {
        DbMqCreateDto createDto = getDbMqCreateDto();
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("table1");
        logicTableConfig.setDstLogicTable("bbz.table1.binlog");
        createDto.setLogicTableConfig(logicTableConfig);
        DbMqConfigInfoDto currentConfig = getCurrentConfig();

        createDto.validAndTrim();
        messengerBatchConfigServiceImpl.processCreateMqConfig(createDto, currentConfig);
        verify(messengerServiceV2, Mockito.times(1)).initMqConfigIfNeeded(ArgumentMatchers.any(), ArgumentMatchers.any());
        verify(dbReplicationTblDao, Mockito.times(1)).batchInsertWithReturnId(ArgumentMatchers.any());
        verify(dbReplicationFilterMappingTblDao, Mockito.times(1)).batchInsert(ArgumentMatchers.any());
    }


    private static DbMqConfigInfoDto getCurrentConfig() {
        DbMqConfigInfoDto currentConfig = new DbMqConfigInfoDto("sha");

        ArrayList<MqLogicTableSummaryDto> logicTableSummaryDtos = new ArrayList<>();
        LogicTableConfig originLogicConfig = new LogicTableConfig();
        originLogicConfig.setLogicTable("table2");
        originLogicConfig.setDstLogicTable("bbz.table2.binlog");
        logicTableSummaryDtos.add(new MqLogicTableSummaryDto(Lists.newArrayList(1L, 2L, 3L, 4L), originLogicConfig));
        currentConfig.setLogicTableSummaryDtos(logicTableSummaryDtos);

        ArrayList<MhaMqDto> mhaMqDtos = new ArrayList<>();
        mhaMqDtos.add(getMhaMqDto("mha1", "sha", getMhaDbReplicationDto(1L, "mha1", "db1")));
        currentConfig.setMhaMqDtos(mhaMqDtos);
        return currentConfig;
    }

    private static MhaMqDto getMhaMqDto(String mha1, String dcName, MhaDbReplicationDto... mhaDbReplicationDtos) {
        MhaMqDto mhaMqDto = new MhaMqDto();
        MhaDto srcMha = new MhaDto();
        srcMha.setName(mha1);
        srcMha.setDcName(dcName);
        mhaMqDto.setSrcMha(srcMha);
        mhaMqDto.setMhaDbReplications(Arrays.asList(mhaDbReplicationDtos));
        return mhaMqDto;
    }

    private static MhaDbReplicationDto getMhaDbReplicationDto(long mhaDbMappingId, String mha1, String dbName) {
        MhaDbReplicationDto mhaDbReplicationDto = new MhaDbReplicationDto();
        mhaDbReplicationDto.setReplicationType(ReplicationTypeEnum.DB_TO_MQ.getType());
        mhaDbReplicationDto.setId(1L);
        mhaDbReplicationDto.setSrc(new MhaDbDto(mhaDbMappingId, mha1, dbName));
        mhaDbReplicationDto.setDst(MhaDbReplicationDto.MQ_DTO);
        ArrayList<DbReplicationDto> dbReplicationDtos = new ArrayList<>();
        mhaDbReplicationDto.setDbReplicationDtos(dbReplicationDtos);
        return mhaDbReplicationDto;
    }

    @Test
    public void testGetLogicTableToMatchedTableMap() throws Exception {
        // Prepare mock data
        List<MhaMqDto> mhaMqDtos = new ArrayList<>();
        MhaMqDto mhaMqDto1 = new MhaMqDto();
        MhaDto srcMha = new MhaDto();
        srcMha.setName("mha1");
        mhaMqDto1.setSrcMha(srcMha);

        List<MhaDbReplicationDto> mha1Db = new ArrayList<>();
        MhaDbReplicationDto mhaDbReplicationDto11 = new MhaDbReplicationDto();
        mhaDbReplicationDto11.setSrc(new MhaDbDto(1L, "mha1", "db11"));
        mha1Db.add(mhaDbReplicationDto11);

        MhaDbReplicationDto mhaDbReplicationDto12 = new MhaDbReplicationDto();
        mhaDbReplicationDto12.setSrc(new MhaDbDto(1L, "mha1", "db12"));
        mha1Db.add(mhaDbReplicationDto12);

        mhaMqDto1.setMhaDbReplications(mha1Db);
        mhaMqDtos.add(mhaMqDto1);

        MhaMqDto mhaMqDto2 = new MhaMqDto();
        MhaDto srcMha2 = new MhaDto();
        srcMha2.setName("mha2");
        mhaMqDto2.setSrcMha(srcMha);


        List<MhaDbReplicationDto> mha2Db = new ArrayList<>();
        MhaDbReplicationDto mhaDbReplicationDto21 = new MhaDbReplicationDto();
        mhaDbReplicationDto21.setSrc(new MhaDbDto(1L, "mha2", "db21"));
        mha2Db.add(mhaDbReplicationDto21);

        MhaDbReplicationDto mhaDbReplicationDto22 = new MhaDbReplicationDto();
        mhaDbReplicationDto22.setSrc(new MhaDbDto(1L, "mha2", "db22"));
        mha2Db.add(mhaDbReplicationDto22);
        mhaMqDto2.setMhaDbReplications(mha2Db);
        mhaMqDtos.add(mhaMqDto2);

        List<LogicTableConfig> logicTableConfigs = new ArrayList<>();
        logicTableConfigs.add(getLogicTableConfig("table1", "bbz.topic.1"));


        Map<String, List<MySqlUtils.TableSchemaName>> res = messengerBatchConfigServiceImpl.getLogicTableToMatchedTableMap(mhaMqDtos, logicTableConfigs);
        assertEquals(1, res.size());
        assertEquals(Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db11", "table1"),
                new MySqlUtils.TableSchemaName("db12", "table1"),
                new MySqlUtils.TableSchemaName("db21", "table1"),
                new MySqlUtils.TableSchemaName("db22", "table1")
        ), res.get("table1"));

        // Verify interactions with the mock
        verify(drcAutoBuildService, times(1)).getMatchTable(anyList());
    }

    @Test
    public void testFilterAndSortRegistryConfig() {
        // Create test data
        MqLogicTableSummaryDto dto1 = new MqLogicTableSummaryDto(Arrays.asList(1L, 3L, 5L), new LogicTableConfig());
        dto1.setExcludeFilterTypes(Collections.emptyList());

        MqLogicTableSummaryDto dto2 = new MqLogicTableSummaryDto(Arrays.asList(2L, 4L, 6L), new LogicTableConfig());
        dto2.setExcludeFilterTypes(Collections.emptyList());
        dto2.setExcludeFilterTypes(Arrays.asList("someFilter"));

        MqLogicTableSummaryDto dto3 = new MqLogicTableSummaryDto(Arrays.asList(7L, 8L, 9L), new LogicTableConfig());

        List<MqLogicTableSummaryDto> inputList = Arrays.asList(dto1, dto2, dto3);

        // Call the method under test
        List<MqLogicTableSummaryDto> result = MessengerBatchConfigServiceImpl.filterAndSortRegistryConfig(inputList);

        // Verify the results
        assertEquals(2, result.size());
        assertEquals(dto1, result.get(0));
        assertEquals(dto3, result.get(1));
    }

    @Test
    public void testGenerateConfig() {
        List<String> dbNames = Lists.newArrayList("db1", "db2");
        List<LogicTableConfig> logicTableConfigs = new ArrayList<>();
        logicTableConfigs.add(getLogicTableConfig("table1", "bbz.topic.1"));
        logicTableConfigs.add(getLogicTableConfig("(table2|table3)", "bbz.topic.23"));
        logicTableConfigs.add(getLogicTableConfig(".*", "bbz.topic.all"));

        Map<String, List<MySqlUtils.TableSchemaName>> logicToMatchTableMap = new HashMap<>();
        logicToMatchTableMap.put("table1", Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table1"),
                new MySqlUtils.TableSchemaName("db2", "table1"))
        );
        logicToMatchTableMap.put("(table2|table3)", Lists.newArrayList(
                new MySqlUtils.TableSchemaName("db1", "table2"),
                new MySqlUtils.TableSchemaName("db1", "table3"),
                new MySqlUtils.TableSchemaName("db2", "table2"),
                new MySqlUtils.TableSchemaName("db2", "table3"))
        );

        logicToMatchTableMap.put(".*", Lists.newArrayList(
                        new MySqlUtils.TableSchemaName("db1", "table1"),
                        new MySqlUtils.TableSchemaName("db1", "table2"),
                        new MySqlUtils.TableSchemaName("db1", "table3"),
                        new MySqlUtils.TableSchemaName("db1", "table4"),
                        new MySqlUtils.TableSchemaName("db1", "table5"),
                        new MySqlUtils.TableSchemaName("db2", "table1"),
                        new MySqlUtils.TableSchemaName("db2", "table2"),
                        new MySqlUtils.TableSchemaName("db2", "table3"),
                        new MySqlUtils.TableSchemaName("db2", "table4"),
                        new MySqlUtils.TableSchemaName("db2", "table5")
                )
        );
        Map<String, String> expectMap = new LinkedHashMap<>() {{
            put("bbz.topic.1.status", "on");
            put("bbz.topic.1.dbName", "db1,db2");
            put("bbz.topic.1.tableName", "table1");
            put("bbz.topic.23.status", "on");
            put("bbz.topic.23.dbName", "db1,db2");
            put("bbz.topic.23.tableName", "table2,table3");
            put("bbz.topic.all.status", "on");
            put("bbz.topic.all.dbName", "db1,db2");
            put("bbz.topic.all.tableName", "table4,table5");
        }};

        Map<String, String> config = MessengerBatchConfigServiceImpl.generateRegistryConfig(dbNames, logicTableConfigs, logicToMatchTableMap);
        assertEquals(expectMap, config);
    }

    private static LogicTableConfig getLogicTableConfig(String table1, String topic1) {
        LogicTableConfig config = new LogicTableConfig();
        config.setLogicTable(table1);
        config.setDstLogicTable(topic1);
        return config;
    }

    private static DbMqCreateDto getDbMqCreateDto() {
        DbMqCreateDto create = new DbMqCreateDto();
        create.setDalclusterName("test_dalcluster");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("new table");
        logicTableConfig.setDstLogicTable("bbz.test.binlog");
        create.setLogicTableConfig(logicTableConfig);
        create.setDbNames(Lists.newArrayList("db1", "db2"));
        create.setSrcRegionName("src1");
        MqConfigDto mqConfig = new MqConfigDto();
        mqConfig.setOrder(true);
        mqConfig.setOrderKey("orderId");
        mqConfig.setBu("bbz");
        mqConfig.setMqType("qmq");
        mqConfig.setSerialization("json");
        create.setMqConfig(mqConfig);

        return create;
    }

    private static DbMqEditDto getDbMqEditDto() {
        DbMqEditDto editDto = new DbMqEditDto();
        editDto.setDalclusterName("test_dalcluster");
        LogicTableConfig logicTableConfig = new LogicTableConfig();
        logicTableConfig.setLogicTable("new table");
        logicTableConfig.setDstLogicTable("bbz.test.binlog");
        editDto.setLogicTableConfig(logicTableConfig);
        LogicTableConfig originLogicTableConfig = new LogicTableConfig();
        originLogicTableConfig.setLogicTable("old_table");
        originLogicTableConfig.setDstLogicTable("bbz.test.binlog");
        originLogicTableConfig.setMessengerFilterId(1L);
        editDto.setOriginLogicTableConfig(originLogicTableConfig);
        editDto.setDbNames(Lists.newArrayList("db1", "db2"));
        editDto.setSrcRegionName("src1");
        MqConfigDto mqConfig = new MqConfigDto();
        mqConfig.setOrder(true);
        mqConfig.setOrderKey("orderId");
        mqConfig.setBu("bbz");
        mqConfig.setMqType("qmq");
        mqConfig.setSerialization("json");
        editDto.setMqConfig(mqConfig);

        editDto.setDbReplicationIds(Lists.newArrayList(1005L, 1006L));
        return editDto;
    }

    public DbMqConfigInfoDto getCurrentCreateConfig(){
        return JsonUtils.fromJson("{\"srcRegionName\":\"src1\",\"dalclusterName\":\"test_dalcluster\",\"dbNames\":[\"db1\",\"db2\"],\"logicTableSummaryDtos\":[{\"mqType\":\"qmq\",\"serialization\":\"json\",\"order\":true,\"orderKey\":\"ticket\",\"persistent\":false,\"dbReplicationIds\":[1005,1006],\"config\":{\"logicTable\":\"old_table\",\"dstLogicTable\":\"bbz.test.binlog\",\"messengerFilterId\":1}}],\"mhaMqDtos\":[{\"srcMha\":{\"name\":\"mha1\",\"regionName\":\"src1\",\"replicatorInfoDtos\":[]},\"mhaDbReplications\":[{\"id\":1,\"src\":{\"mhaDbMappingId\":1,\"mhaName\":\"mha1\",\"dbName\":\"db1\",\"regionName\":\"src1\"},\"dst\":{\"mhaDbMappingId\":-1},\"replicationType\":1,\"dbReplicationDtos\":[{\"dbReplicationId\":1005,\"logicTableConfig\":{\"logicTable\":\"old_table\",\"dstLogicTable\":\"bbz.test.binlog\",\"messengerFilterId\":1}}],\"dbApplierDto\":{\"ips\":[\"1.113.60.1\",\"1.113.60.2\"],\"gtidInit\":\"gtid1\",\"dbName\":\"db1\"}},{\"id\":2,\"src\":{\"mhaDbMappingId\":2,\"mhaName\":\"mha1\",\"dbName\":\"db2\",\"regionName\":\"src1\"},\"dst\":{\"mhaDbMappingId\":-1},\"replicationType\":1,\"dbReplicationDtos\":[{\"dbReplicationId\":1006,\"logicTableConfig\":{\"logicTable\":\"old_table\",\"dstLogicTable\":\"bbz.test.binlog\",\"messengerFilterId\":1}}],\"dbApplierDto\":{\"ips\":[],\"gtidInit\":\"gtid2\",\"dbName\":\"db2\"}}]}]}", DbMqConfigInfoDto.class);
    }
}


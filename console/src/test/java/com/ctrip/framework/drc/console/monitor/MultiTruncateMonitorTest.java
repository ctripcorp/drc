package com.ctrip.framework.drc.console.monitor;

import static com.ctrip.framework.drc.console.monitor.MockTest.times;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.DdlHistoryTblDao;
import com.ctrip.framework.drc.console.dao.entity.DdlHistoryTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.core.monitor.reporter.Reporter;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class MultiTruncateMonitorTest {

    @InjectMocks
    private MultiTruncateMonitor multiTruncateMonitor;
    @Mock
    private DdlHistoryTblDao ddlHistoryTblDao;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MhaDbReplicationService mhaDbReplicationService;
    @Mock
    private Reporter reporter;

    String mhaTblsJson = "[\n"
            + "  {\n"
            + "    \"id\": 1,\n"
            + "    \"mhaName\": \"mha1\",\n"
            + "    \"clusterName\": \"test_dalcluster\",\n"
            + "    \"dcId\": 1,\n"
            + "    \"buId\": 1,\n"
            + "    \"monitorSwitch\": 0,\n"
            + "    \"applyMode\": 0,\n"
            + "    \"deleted\": 0\n"
            + "  },\n"
            + "  {\n"
            + "    \"id\": 2,\n"
            + "    \"mhaName\": \"mha2\",\n"
            + "    \"clusterName\": \"test_dalcluster\",\n"
            + "    \"dcId\": 2,\n"
            + "    \"buId\": 1,\n"
            + "    \"monitorSwitch\": 0,\n"
            + "    \"applyMode\": 0,\n"
            + "    \"deleted\": 0\n"
            + "  },\n"
            + "  {\n"
            + "    \"id\": 3,\n"
            + "    \"mhaName\": \"mha3\",\n"
            + "    \"clusterName\": \"test_dalcluster\",\n"
            + "    \"dcId\": 1,\n"
            + "    \"buId\": 1,\n"
            + "    \"monitorSwitch\": 0,\n"
            + "    \"applyMode\": 0,\n"
            + "    \"deleted\": 0\n"
            + "  },\n"
            + "  {\n"
            + "    \"id\": 4,\n"
            + "    \"mhaName\": \"mha4\",\n"
            + "    \"clusterName\": \"test_dalcluster\",\n"
            + "    \"dcId\": 3,\n"
            + "    \"buId\": 1,\n"
            + "    \"monitorSwitch\": 0,\n"
            + "    \"applyMode\": 0,\n"
            + "    \"deleted\": 0\n"
            + "  }\n"
            + "]";
    String ddlTblsJson = "[\n"
            + "    {\n"
            + "        \"id\": 1,\n"
            + "        \"mhaId\": 1,\n"
            + "        \"ddl\": \"truncate table mysql_cluster\",\n"
            + "        \"schemaName\": \"opstestdatadb\",\n"
            + "        \"tableName\": \"mysql_cluster\",\n"
            + "        \"queryType\": 4,\n"
            + "        \"createTime\": \"2021-03-18 15:00:00\"\n"
            + "    },\n"
            + "    {\n"
            + "        \"id\": 2,\n"
            + "        \"mhaId\": 2,\n"
            + "        \"ddl\": \"truncate table migrationdb.benchmark\",\n"
            + "        \"schemaName\": \"migrationdb\",\n"
            + "        \"tableName\": \"benchmark\",\n"
            + "        \"queryType\": 4\n"
            + "    },\n"
            + "    {\n"
            + "        \"id\": 3,\n"
            + "        \"mha_id\": 3,\n"
            + "        \"ddl\": \"truncate table migrationdb.benchmark\",\n"
            + "        \"schemaName\": \"migrationdb\",\n"
            + "        \"tableName\": \"benchmark\",\n"
            + "        \"queryType\": 4\n"
            + "    }\n"
            + "]";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
       
    }

    @Test
    public void testScheduledTask() throws SQLException {
        multiTruncateMonitor.isRegionLeader = true;
        List<MhaTblV2> mhaTblV2s  = JsonUtils.fromJsonToList(mhaTblsJson, MhaTblV2.class);
        List<DdlHistoryTbl> ddlHistoryTbls = JsonUtils.fromJsonToList(ddlTblsJson, DdlHistoryTbl.class);
        when(consoleConfig.isCenterRegion()).thenReturn(true);
        when(ddlHistoryTblDao.queryByStartCreateTime(any())).thenReturn(ddlHistoryTbls);
        List<MhaTblV2> migrationdbMha = mhaTblV2s.stream().filter(mhaTblV2 -> mhaTblV2.getId() == 2 || mhaTblV2.getId() == 3)
                .collect(Collectors.toList());
        List<MhaTblV2> opstestdatadbMha = mhaTblV2s.stream().filter(mhaTblV2 -> mhaTblV2.getId() == 1|| mhaTblV2.getId() == 4)
                .collect(Collectors.toList());
        when(mhaDbReplicationService.getReplicationRelatedMha(eq("migrationdb"),eq("benchmark"))).thenReturn(migrationdbMha);
        when(mhaDbReplicationService.getReplicationRelatedMha(eq("opstestdatadb"),eq("mysql_cluster"))).thenReturn(opstestdatadbMha);
        when(ddlHistoryTblDao.queryByDbAndTime(eq("opstestdatadb"),eq("mysql_cluster"),any(),any())).thenReturn(Lists.newArrayList());
        multiTruncateMonitor.scheduledTask();
        Mockito.verify(reporter, times(1)).reportResetCounter(any(),eq(0L),anyString());
        Mockito.verify(reporter, times(1)).reportResetCounter(any(),eq(1L),anyString());
        
    }
}